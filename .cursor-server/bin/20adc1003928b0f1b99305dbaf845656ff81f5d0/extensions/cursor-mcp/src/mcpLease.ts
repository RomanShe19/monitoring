import * as vscode from 'vscode';
import type {
	McpClient,
	McpLease,
	McpToolResult,
	NamedMcpToolDefinition,
} from '@anysphere/agent-exec';
import type { Context } from '@anysphere/context';
import { ExecutableMcpTool, ExecutableMcpToolSet, McpManager, type McpElicitationProviderFactory } from '@anysphere/local-exec';
import type { JsonValue } from '@bufbuild/protobuf';
import type { McpProvider } from '@cursor/types';
import { McpInstructions } from './proto/agent/v1/mcp_pb.js';
import { getMcpManager } from './commands/mcpCommands.js';
import { z } from 'zod';

// Schema for JSON Schema property definition
const JsonSchemaPropertySchema = z.object({
	description: z.string().optional(),
}).passthrough();

// Schema for JSON Schema parameters object
const JsonSchemaParametersSchema = z.object({
	properties: z.record(z.string(), JsonSchemaPropertySchema).optional(),
	required: z.array(z.string()).optional(),
}).passthrough();

export class VscodeMcpLease implements McpLease {
	private readonly mcpManager: McpManager;

	constructor(
		private readonly context: vscode.ExtensionContext,
	) {
		this.mcpManager = getMcpManager();

		this.initializeMcpProviders();

		this.context.subscriptions.push(
			vscode.cursor.onDidRegisterMcpProvider((provider) => {
				this.registerMcpProvider(provider)
			})
		);

		// If main thread broadcasts an unregistration event for a remote mcp provider, delete the client from the local manager
		this.context.subscriptions.push(
			vscode.cursor.onDidUnregisterMcpProvider((id) => {
				this.mcpManager.deleteClient(id);
			})
		);
	}

	async getClients(): Promise<Record<string, McpClient>> {
		return this.mcpManager.getClients();
	}

	async getClient(name: string): Promise<McpClient | undefined> {
		return this.mcpManager.getClient(name);
	}

	async getInstructions(ctx: Context): Promise<McpInstructions[]> {
		return this.mcpManager.getInstructions(ctx);
	}

	private async initializeMcpProviders(): Promise<void> {
		try {
			const providers = await vscode.cursor.getAllMcpProviders();

			for (const provider of providers) {
				await this.registerMcpProviderToManager(this.mcpManager, provider);
			}
		} catch (error) {
			console.error('Failed to initialize MCP providers:', error);
		}
	}

	private async registerMcpProvider(provider: McpProvider): Promise<void> {
		try {
			await this.registerMcpProviderToManager(this.mcpManager, provider);
		} catch (error) {
			console.error(`Failed to register MCP provider ${provider.id}:`, error);
		}
	}

	private async registerMcpProviderToManager(
		mcpManager: McpManager,
		provider: McpProvider
	): Promise<void> {
		const clientWrapper: McpClient = {
			getState: async (_ctx: Context) => ({ kind: 'ready' as const }),
			getTools: async (_ctx: Context) => {
				const offerings = await provider.listOfferings();
				if (!offerings?.tools) {
					return [];
				}
				return offerings.tools.map((tool) => ({
					name: tool.name,
					description: tool.description,
					inputSchema: JSON.parse(tool.parameters),
				}));
			},
			callTool: async (
				_ctx: Context,
				toolName: string,
				args: Record<string, unknown>,
				_toolCallId?: string,
				_elicitationProvider?: any
			) => {
				return provider.callTool(toolName, args) as ReturnType<
					McpClient['callTool']
				>;
			},
			getInstructions: async (ctx: Context) => {
				return undefined;
			},
			listResources: async (_ctx: Context) => {
				const offerings = await provider.listOfferings();
				if (!offerings?.resources) {
					return { resources: [] };
				}
				return { resources: offerings.resources };
			},
			readResource: async (_ctx: Context, _args: { uri: string }) => {
				return { contents: [] };
			},
			listPrompts: async (_ctx: Context) => {
				const offerings = await provider.listOfferings();
				if (!offerings?.prompts) {
					return [];
				}
				return offerings.prompts.map((prompt) => {
					let argumentsList: Array<{ name: string; description?: string; required: boolean }> = [];

					if (prompt.parameters) {
						const parsed = JSON.parse(prompt.parameters);
						const result = JsonSchemaParametersSchema.safeParse(parsed);

						if (result.success && result.data.properties) {
							const requiredSet = new Set(result.data.required ?? []);
							argumentsList = Object.entries(result.data.properties).map(([name, prop]) => ({
								name,
								description: prop.description,
								required: requiredSet.has(name),
							}));
						}
					}

					return {
						name: prompt.name,
						description: prompt.description,
						arguments: argumentsList,
					};
				});
			},
			getPrompt: async (_ctx: Context, _name: string, _args?: Record<string, string>) => {
				return { messages: [] };
			},
			serverName: provider.name ?? provider.id,
			// Config is not available for extension-based MCP providers
			// It will be available for config-based MCP clients (stdio/http)
			config: undefined,
		};

		mcpManager.setClient(provider.id, clientWrapper);
	}

	async getToolSet(ctx: Context): Promise<ExecutableMcpToolSet> {
		const enabledToolsByServer = await vscode.cursor.getEnabledMcpTools();

		const allClients = await this.getClients();


		// Filter to only clients that have enabled tools
		const enabledClients = Object.fromEntries(
			Object.entries(allClients).filter(([key, client]) =>
				enabledToolsByServer[key] !== undefined
			)
		);

		const clientTools = await Promise.all(
			Object.entries(enabledClients).map(([key, client]) =>
				client
					.getTools(ctx)
					.then((tools) =>
						tools.map((tool) => ({
							...tool,
							// Use key (identifier) for filtering, serverName for display/providerIdentifier
							clientKey: key,
							clientName: client.serverName,
							client: client,
						}))
					)
					.catch(() => [])
			)
		);

		// Filter to only include enabled tools using the same mechanism as mcpService
		// enabledToolsByServer maps server identifier (key) -> list of enabled tool names
		const tools = clientTools.flat().filter((tool) => {
			const enabledToolsForServer = enabledToolsByServer[tool.clientKey];
			if (!enabledToolsForServer) {
				return false; // No enabled tools list means server not enabled or not found
			}
			return enabledToolsForServer.includes(tool.name);
		});

		const toolsMap: Record<string, ExecutableMcpTool> = {};
		for (const tool of tools) {
			// Use clientKey (identifier) for the map key, clientName (display name) for providerIdentifier
			toolsMap[`${tool.clientKey}-${tool.name}`] = {
				definition: {
					...tool,
					providerIdentifier: tool.clientName,
					toolName: tool.name,
				},
				execute: async (
					args: Record<string, JsonValue>,
					toolCallId?: string,
					elicitationFactory?: McpElicitationProviderFactory
				) => {
					const elicitationProvider = elicitationFactory?.createProvider(
						tool.clientName,
						tool.name,
						toolCallId
					);

					const result = await tool.client.callTool(
						ctx,
						tool.name,
						args,
						toolCallId,
						elicitationProvider
					);
					return result;
				},
			};
		}

		return new ExecutableMcpToolSet(toolsMap);
	}

	async getTools(ctx: Context): Promise<NamedMcpToolDefinition[]> {
		const toolSet = await this.getToolSet(ctx);
		return toolSet.getTools();
	}
}
