import * as vscode from 'vscode';
import * as http from 'http';
import * as fs from 'fs';
import { dirname } from 'path';
import { Transform, pipeline } from 'stream';
import { NdjsonIngestLogger } from './logger.js';

let server: http.Server | undefined;
let currentPort: number = 0;
let logPath: string = '';
let externalUrl: string = '';

type ServerConfig = {
	externalUrl: string;
	logPath: string;
};

const PORT_RANGE_START: number = 7242;
const PORT_RANGE_END: number = 7942;

function isPortAvailable(port: number, bindAddress: string): Promise<boolean> {
	return new Promise((resolve) => {
		const testServer: http.Server = http.createServer();
		testServer.once('error', () => resolve(false));
		testServer.once('listening', () => {
			testServer.close(() => resolve(true));
		});
		testServer.listen(port, bindAddress);
	});
}

async function choosePort(context: vscode.ExtensionContext, bindAddress: string): Promise<number> {
	const cfg: vscode.WorkspaceConfiguration = vscode.workspace.getConfiguration('ndjson');
	const configuredPort: number = cfg.get<number>('port', 0);

	// User-configured port in workspace settings takes priority
	if (configuredPort !== 0) {
		return configuredPort;
	}

	// Check for previously auto-allocated port in internal storage
	const previouslyAllocated: number = context.workspaceState.get<number>('allocatedPort', 0);
	if (previouslyAllocated !== 0) {
		return previouslyAllocated;
	}

	// Auto-allocate: start from last allocated + 1, or range start
	const lastAllocated: number = context.globalState.get<number>('lastAllocatedPort', PORT_RANGE_START - 1);
	const rangeSize: number = PORT_RANGE_END - PORT_RANGE_START + 1;

	// Try ports in range, wrapping around
	for (let i = 0; i < rangeSize; i++) {
		const candidate: number = PORT_RANGE_START + ((lastAllocated - PORT_RANGE_START + 1 + i) % rangeSize);

		if (await isPortAvailable(candidate, bindAddress)) {
			// Remember globally for distributing ports across workspaces
			await context.globalState.update('lastAllocatedPort', candidate);
			// Persist to internal workspace storage (doesn't touch .vscode/settings.json)
			await context.workspaceState.update('allocatedPort', candidate);
			NdjsonIngestLogger.info(`Auto-allocated port ${candidate} and saved to internal storage`);
			return candidate;
		}
	}

	throw new Error(`No available ports in range ${PORT_RANGE_START}-${PORT_RANGE_END}`);
}

function ensureTrailingNewline(): Transform {
	let last: number = 0x0a;
	return new Transform({
		transform(chunk: Buffer, _enc: string, cb: (error?: Error | null, data?: Buffer) => void): void {
			last = chunk[chunk.length - 1];
			cb(null, chunk);
		},
		flush(cb: (error?: Error | null) => void): void {
			if (last !== 0x0a) {
				this.push(Buffer.from('\n'));
			}
			cb();
		}
	});
}

let writeQueue: Promise<void> = Promise.resolve();

async function appendExclusive(src: NodeJS.ReadableStream, destPath: string): Promise<void> {
	const run = (): Promise<void> => new Promise<void>((resolve, reject) => {
		const out: fs.WriteStream = fs.createWriteStream(destPath, { flags: 'a' });
		pipeline(src, ensureTrailingNewline(), out, (err: NodeJS.ErrnoException | null) => {
			if (err) {
				reject(err);
			} else {
				resolve();
			}
		});
	});

	const p: Promise<void> = writeQueue.then(run, run);
	writeQueue = p.catch((e) => {
		NdjsonIngestLogger.error('Error appending to log file:', e);
	});
	return p;
}

async function startServer(context: vscode.ExtensionContext): Promise<{ externalUrl: string, logPath: string } | string | undefined> {
	if (!vscode.workspace.isTrusted) {
		vscode.window.showWarningMessage('debug mode disabled in untrusted workspace.');
		return;
	}

	const ws: vscode.WorkspaceFolder | undefined = vscode.workspace.workspaceFolders?.[0];
	if (!ws) {
		vscode.window.showErrorMessage('No workspace storage available (open a folder/workspace).');
		return;
	}

	if (server) {
		return { externalUrl: externalUrl, logPath: logPath };
	}

	const cfg = vscode.workspace.getConfiguration('ndjson');
	const relativeLogFile = cfg.get<string>('relativeLogFile', '.cursor/debug.log');
	const logUri: vscode.Uri = vscode.Uri.joinPath(ws.uri, relativeLogFile);
	fs.mkdirSync(dirname(logUri.fsPath), { recursive: true });
	logPath = logUri.fsPath;

	const id = context.workspaceState.get<string>("nrdjson.targetId", crypto.randomUUID());
	context.workspaceState.update("nrdjson.targetId", id);

	server = http.createServer((req: http.IncomingMessage, res: http.ServerResponse) => {
		res.setHeader('Access-Control-Allow-Origin', '*');
		res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
		res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

		if (req.method === 'OPTIONS') {
			res.writeHead(200);
			res.end();
			return;
		}

		if (req.method !== 'POST' || req.url !== `/ingest/${id}`) {
			res.writeHead(404);
			res.end();
			return;
		}

		appendExclusive(req, logPath)
			.then(() => {
				res.writeHead(204);
				res.end();
			})
			.catch((err: Error) => {
				NdjsonIngestLogger.error('NDJSON ingest write error:', err);
				res.writeHead(500);
				res.end('write-failed');
			});
	});

	const bindAddress: string = cfg.get<string>('bindAddress', '127.0.0.1');
	const isAutoAllocating: boolean = cfg.get<number>('port', 0) === 0;

	let port: number;
	try {
		port = await choosePort(context, bindAddress);
	} catch (e) {
		const errorMessage: string = e instanceof Error ? e.message : String(e);
		NdjsonIngestLogger.error('Failed to choose port:', e);
		vscode.window.showErrorMessage(`NDJSON Ingest: ${errorMessage}`);
		return errorMessage;
	}
	let readyCallback: (configOrError: ServerConfig | string) => void;
	const readyPromise = new Promise<ServerConfig | string>(resolve => readyCallback = resolve);
	server.once('error', (err: NodeJS.ErrnoException) => {
		switch (err.code) {
			case 'EADDRINUSE':
				NdjsonIngestLogger.error(`Port ${port} is already in use`);
				if (isAutoAllocating) {
					// Race condition: port became unavailable after we checked
					vscode.window.showErrorMessage(
						`NDJSON Ingest: Cannot start server - port ${port} became unavailable. Please try again.`
					);
				} else {
					vscode.window.showErrorMessage(
						`NDJSON Ingest: Cannot start server - port ${port} is already in use. ` +
						`Try changing the port in settings or stopping the other service.`,
						'Change Port'
					).then((selection: string | undefined) => {
						if (selection === 'Change Port') {
							vscode.commands.executeCommand('workbench.action.openWorkspaceSettings', 'ndjson.port');
						}
					});
				}
				readyCallback('Port is already in use');
				break;
			case 'EACCES':
				NdjsonIngestLogger.error(`Permission denied for port ${port}`);
				if (isAutoAllocating) {
					vscode.window.showErrorMessage(
						`NDJSON Ingest: Permission denied for port ${port}. This is unexpected for auto-allocated ports.`
					);
				} else {
					vscode.window.showErrorMessage(
						`NDJSON Ingest: Permission denied for port ${port}. ` +
						`Try using a port number greater than 1024.`,
						'Change Port'
					).then((selection: string | undefined) => {
						if (selection === 'Change Port') {
							vscode.commands.executeCommand('workbench.action.openWorkspaceSettings', 'ndjson.port');
						}
					});
				}
				readyCallback(`Permission denied for port ${port}`);
				break;
			default:
				NdjsonIngestLogger.error('NDJSON ingest server listen error:', err);
				vscode.window.showErrorMessage(`NDJSON Ingest: Server error - ${err.message}`);
				readyCallback(`Server error - ${err.message}`);
		}
		server = undefined;
	});
	server.listen(port, bindAddress, async () => {
		const addr: string | import('net').AddressInfo | null = server!.address();
		currentPort = typeof addr === 'object' && addr ? addr.port : 0;

		const loopback: vscode.Uri = vscode.Uri.parse(`http://${bindAddress}:${currentPort}/ingest/${id}`);
		try {
			const external: vscode.Uri = await vscode.env.asExternalUri(loopback);
			externalUrl = external.toString();
		} catch (e) {
			NdjsonIngestLogger.error('asExternalUri failed; using loopback URL', e);
			externalUrl = loopback.toString();
		}

		try {
			if (fs.existsSync(logPath)) {
				fs.unlinkSync(logPath);
			}
		} catch (e) {
			NdjsonIngestLogger.error('Error clearing existing log file:', e);
		}

		NdjsonIngestLogger.info(`NDJSON ingest server started on port ${currentPort}`);
		readyCallback({ externalUrl: externalUrl, logPath: logPath });
	});
	return readyPromise;
}

async function stopServer() {
	NdjsonIngestLogger.info('Stopping NDJSON ingest server');
	if (server) {
		await new Promise<void>((resolve) => server!.close(() => resolve())).catch((err) => {
			NdjsonIngestLogger.error('Error stopping NDJSON ingest server:', err);
		});
		server = undefined;
	}
	// Do not delete the server info file on stop; if another process is running,
	// it remains correct, and if none are running, harmlessly stale.
}

export async function activate(context: vscode.ExtensionContext) {
	NdjsonIngestLogger.init();
	context.subscriptions.push(vscode.commands.registerCommand('cursor.ndjsonIngest.start', () => startServer(context)));
	context.subscriptions.push(vscode.commands.registerCommand('cursor.ndjsonIngest.stop', () => stopServer()));
	context.subscriptions.push(vscode.commands.registerCommand('cursor.ndjsonIngest.copyCurl', async () => {
		if (!externalUrl) { vscode.window.showWarningMessage('Server is not running.'); return; }
		const cmd = `curl -sS -H "Content-Type: application/x-ndjson" --data-binary '{"hello":"world"}' "${externalUrl.toString()}"`;
		await vscode.env.clipboard.writeText(cmd);
		vscode.window.showInformationMessage('NDJSON Ingest: curl command copied to clipboard.');
	}));
	context.subscriptions.push(vscode.commands.registerCommand('cursor.ndjsonIngest.showStatus', () => {
		if (!externalUrl) { vscode.window.showInformationMessage('NDJSON: server not running.'); return; }
		vscode.window.showInformationMessage(`URL: ${externalUrl.toString()}`);
	}));
}

export async function deactivate(): Promise<void> {
	await stopServer();
}
