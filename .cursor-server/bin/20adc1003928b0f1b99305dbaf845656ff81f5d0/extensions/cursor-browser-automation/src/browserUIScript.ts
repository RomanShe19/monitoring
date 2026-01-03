/*---------------------------------------------------------------------------------------------
 *  Copyright (c) Anysphere. All rights reserved.
 *--------------------------------------------------------------------------------------------*/

/**
 * Generate the minimal browser UI injection script for element selection.
 * This script runs in the browser page context and handles:
 * - Area screenshot selection with drag
 * - Keyboard shortcuts
 *
 * Note: Element selection overlay is handled by the shared browserOverlay.ts
 * Note: Console logging is captured natively by Electron's console-message event.
 */
export function generateBrowserUIScript(tabId: string): string {
	return `
(function() {
	// Prevent double injection
	if (window.__cursorBrowserAutomationInjected) {
		return;
	}

	// Only inject in top-level frame
	if (window !== window.top) {
		return;
	}

	window.__cursorBrowserAutomationInjected = true;

	const tabId = ${JSON.stringify(tabId)};

	// =============================================================================
	// Area Screenshot Selection
	// =============================================================================
	// IMPORTANT: BrowserEditorContent.tsx may also inject area screenshot handling
	// via BROWSER_AREA_SCREENSHOT_SCRIPT. Check if it's already injected to avoid
	// duplicate screenshot captures.

	const areaScreenshotAlreadyInjected = window.__cursorAreaScreenshotInjected === true;

	let isDragging = false;
	let dragStartX = null;
	let dragStartY = null;
	let dragSelectionBox = null;
	let areaDragListeners = null;

	function enableAreaSelection() {
		// Skip if BrowserEditorContent already handles area screenshots
		if (areaScreenshotAlreadyInjected) {
			return;
		}

		const mousedownListener = (e) => {
			// Only start drag if not clicking on any overlay elements
			if (e.target.getAttribute &&
				e.target.getAttribute('data-cursor-overlay') === 'true'
			) {
				return;
			}

			e.preventDefault();
			e.stopPropagation();

			isDragging = true;
			dragStartX = e.clientX;
			dragStartY = e.clientY;

			// Hide any existing overlays
			const overlays = document.querySelectorAll('[data-cursor-overlay="true"]');
			overlays.forEach(el => el.style.display = 'none');

			if (!dragSelectionBox) {
				dragSelectionBox = document.createElement('div');
				dragSelectionBox.style.cssText = 'position:fixed;background:rgba(58,150,221,0.1);border:2px dashed #3a96dd;pointer-events:none;z-index:2147483647;';
				document.body.appendChild(dragSelectionBox);
			}
			dragSelectionBox.style.left = dragStartX + 'px';
			dragSelectionBox.style.top = dragStartY + 'px';
			dragSelectionBox.style.width = '0px';
			dragSelectionBox.style.height = '0px';
		};

		const mousemoveListener = (e) => {
			if (!isDragging || !dragSelectionBox) return;

			const currentX = e.clientX;
			const currentY = e.clientY;

			const left = Math.min(dragStartX, currentX);
			const top = Math.min(dragStartY, currentY);
			const width = Math.abs(currentX - dragStartX);
			const height = Math.abs(currentY - dragStartY);

			dragSelectionBox.style.left = left + 'px';
			dragSelectionBox.style.top = top + 'px';
			dragSelectionBox.style.width = width + 'px';
			dragSelectionBox.style.height = height + 'px';
		};

		const mouseupListener = (e) => {
			if (!isDragging) return;
			e.preventDefault();
			e.stopPropagation();

			const currentX = e.clientX;
			const currentY = e.clientY;

			const left = Math.min(dragStartX, currentX);
			const top = Math.min(dragStartY, currentY);
			const width = Math.abs(currentX - dragStartX);
			const height = Math.abs(currentY - dragStartY);

			if (width > 5 || height > 5) {
				const bounds = {
					x: Math.round(left),
					y: Math.round(top),
					width: Math.round(width),
					height: Math.round(height)
				};

				if (dragSelectionBox) {
					dragSelectionBox.remove();
					dragSelectionBox = null;
				}

				disableAreaSelection();

				// Send area screenshot selection
				requestAnimationFrame(() => {
					requestAnimationFrame(() => {
						if (window.cursorBrowser) {
							window.cursorBrowser.send('area-screenshot-selected', {
								bounds: bounds
							});
						}

						// Area selection is complete, element selection will continue to work
						// No need to re-enable as the overlay system maintains its own state
					});
				});
			} else {
				if (dragSelectionBox) {
					dragSelectionBox.remove();
					dragSelectionBox = null;
				}
			}

			isDragging = false;
		};

		document.addEventListener('mousedown', mousedownListener, true);
		document.addEventListener('mousemove', mousemoveListener);
		document.addEventListener('mouseup', mouseupListener, true);

		areaDragListeners = {
			mousedown: mousedownListener,
			mousemove: mousemoveListener,
			mouseup: mouseupListener
		};
	}

	function disableAreaSelection() {
		// Skip if BrowserEditorContent already handles area screenshots
		if (areaScreenshotAlreadyInjected) {
			return;
		}

		isDragging = false;
		dragStartX = null;
		dragStartY = null;

		if (dragSelectionBox) {
			dragSelectionBox.remove();
			dragSelectionBox = null;
		}

		if (areaDragListeners) {
			const { mousedown, mousemove, mouseup } = areaDragListeners;
			document.removeEventListener('mousedown', mousedown, true);
			document.removeEventListener('mousemove', mousemove);
			document.removeEventListener('mouseup', mouseup, true);
			areaDragListeners = null;
		}
	}

	// =============================================================================
	// Message Handling
	// =============================================================================

	window.addEventListener('message', (e) => {
		if (e.data.type === 'enable-element-selection') {
			// Check if we should enable area selection along with element selection
			if (e.data.enableAreaSelection) {
				enableAreaSelection();
			}
			// Don't re-post the message - the overlay system will handle it directly
		} else if (e.data.type === 'disable-element-selection') {
			disableAreaSelection();
			// Don't re-post the message - the overlay system will handle it directly
		} else if (e.data.type === 'start-area-screenshot') {
			enableAreaSelection();
		} else if (e.data.type === 'stop-area-screenshot') {
			disableAreaSelection();
		}
	});

	// =============================================================================
	// Keyboard Shortcuts
	// =============================================================================

	document.addEventListener('keydown', (e) => {
		const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
		const cmdOrCtrl = isMac ? e.metaKey : e.ctrlKey;

		if (cmdOrCtrl && !e.altKey) {
			if (e.key === 'a' && !e.shiftKey) {
				e.preventDefault();
				e.stopPropagation();

				const target = e.target;
				if (target instanceof HTMLInputElement || target instanceof HTMLTextAreaElement) {
					target.select();
				} else if (target instanceof HTMLElement && target.isContentEditable) {
					const selection = window.getSelection();
					const range = document.createRange();
					range.selectNodeContents(target);
					selection?.removeAllRanges();
					selection?.addRange(range);
				} else {
					document.execCommand('selectAll');
				}
				return;
			}
		}
	}, true);

})();
`;
}