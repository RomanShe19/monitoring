import * as vscode from 'vscode';
import type { BrowserToolContext } from './browser-tool.js';

const NAVIGATION_COMMAND = 'cursor.browserOriginAllowlist.ensureNavigationAllowed';
const PAGE_STATE_COMMAND = 'cursor.browserOriginAllowlist.ensurePageOriginAllowed';

export async function ensureBrowserNavigationAllowed(url: string): Promise<void> {
	await vscode.commands.executeCommand(NAVIGATION_COMMAND, { url });
}

export async function ensureBrowserPageAllowed(toolName: string, context: BrowserToolContext): Promise<void> {
	const currentTab = context.currentTab();
	const currentUrl = currentTab ? currentTab.page.url() : undefined;
	await vscode.commands.executeCommand(PAGE_STATE_COMMAND, { toolName, url: currentUrl });
}

