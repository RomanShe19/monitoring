import { ExtensionContext } from 'vscode';
import { McpLogger } from './utils/logger';

/** Get a server-specific logger instance */
function getLogger(identifier: string) {
	return McpLogger.getLogger(identifier);
}
import * as vscode from 'vscode';
import * as os from 'os';
import { registerContext } from './commands/mcpCommands.js';
import { MCPOAuthClientProvider, decodeOAuthState } from './commands/mcp/oauth.js';
import { auth } from '@modelcontextprotocol/sdk/client/auth.js';
import { EverythingProviderCreator } from './everythingProvider.js';
import { VscodeMcpLease } from './mcpLease.js';
import type { McpLease } from '@anysphere/agent-exec';


const deactivateTasks: { (): Promise<any> }[] = [];

export interface CursorMcpExtensionApi {
	getMcpLease(): McpLease;
}

// this method is called when vs code is activated
export function activate(context: ExtensionContext): CursorMcpExtensionApi {
	const everythingProviderCreator = new EverythingProviderCreator(context);
	deactivateTasks.push(async () => everythingProviderCreator.dispose());

	McpLogger.init();
	registerContext(context)

	// Register a URI handler to receive deep-link callbacks such as
	//  {protocol}://anysphere.cursor-mcp/oauth/callback?code=...&state=... (e.g., cursor://, cursor-nightly://, cursor-dev://)
	const uriHandlerDisp = vscode.window.registerUriHandler({
		handleUri: async (uri: vscode.Uri) => {
			try {
				// Expected path format: /oauth/callback (static URL per OAuth spec)
				const [, first, second] = uri.path.split('/');
				if (first !== 'oauth' || second !== 'callback') {
					vscode.window.showWarningMessage('Unrecognized deep link. Try updating Cursor');
					return;
				}

				// Extract parameters from the callback URL
				const params = new URLSearchParams(uri.query);

				// Extract the identifier from the OAuth state parameter
				const stateParam = params.get('state');
				if (!stateParam) {
					McpLogger.error('OAuth callback received without state parameter');
					vscode.window.showErrorMessage('OAuth callback is missing required state parameter. Please try again.');
					return;
				}

				const statePayload = decodeOAuthState(stateParam);
				if (!statePayload) {
					McpLogger.error('OAuth callback received with invalid state parameter');
					vscode.window.showErrorMessage('OAuth callback has invalid state parameter. Please try again.');
					return;
				}

				const identifier = statePayload.id;

				// Extract ?code=... but preserve URL-encoded format
				const codeDecoded = params.get('code');
				if (!codeDecoded) {
					getLogger(identifier).warn('OAuth callback received without code parameter');
					return;
				}

				//  URLSearchParams.get() automatically decodes, but OAuth servers
				// (like Atlassian) expect the authorization code to remain URL-encoded.
				// so lets pull things out exactly from the url so we respect whatever the
				// server who sent it to us is expecting
				const codeMatch = uri.query.match(/(?:^|&)code=([^&]*)/);
				const code = codeMatch ? codeMatch[1] : codeDecoded;

				getLogger(identifier).info(`Received OAuth callback with code`);

				if (!code) {
					getLogger(identifier).error(`Authorization code not found in redirect url callback`);
					return;
				}

				// Retrieve stored server URL
				const serverUrl = context.globalState.get<string>(`[${identifier}] ${'mcp_server_url'}`);
				if (!serverUrl) {
					getLogger(identifier).error(`No stored server URL for OAuth flow`);
					return;
				}

				// Notify UI that we're now exchanging the token
				await vscode.commands.executeCommand('mcp.updateStatus', {
					identifier,
					status: { type: 'initializing' }
				});

				// Exchange the code for tokens via the SDK helper
				try {
					const provider = new MCPOAuthClientProvider(context, serverUrl, identifier, () => {});
					await auth(provider, {
						serverUrl,
						authorizationCode: code,
					});
					getLogger(identifier).info(`OAuth authorization completed`);
					// Clear status to allow reconnect
					await vscode.commands.executeCommand('mcp.reloadClient', { identifier, serverInfo: { type: 'streamableHttp', serverUrl } });
				} catch (e) {
					getLogger(identifier).error(`Failed to complete OAuth exchange`, e as Error);
				}
			} catch (err) {
				// Use general logger for general errors
				McpLogger.error('Error handling OAuth callback URI', err as Error);
			}
		}
	});
	deactivateTasks.push(async () => uriHandlerDisp.dispose());

	const mcpLease = new VscodeMcpLease(context);

	return {
		getMcpLease: () => mcpLease,
	};
}

export async function deactivate(): Promise<void> {
	for (const task of deactivateTasks) {
		await task();
	}
}
