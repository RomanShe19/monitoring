// Copyright Anysphere Inc.
import { MethodKind } from '@bufbuild/protobuf';
import {
	Code, ConnectError
} from '@connectrpc/connect';
import {
	afterEach,
	beforeEach,
	describe,
	expect,
	it,
	vi
} from 'vitest';
import {
	RewindableAsyncIterable,
	calculateBackoffDelay,
	createStreamRetryInterceptor,
	createUnaryRetryInterceptor,
	shouldRetryOnError,
} from '../../src/aiConnectTransport/retryInterceptor.js';
import {
	StructuredLogProviderImpl
} from '../../src/structuredLogProvider.js';
import {
	CursorDebugLogger
} from '../../src/utils/logger.js';

// Mock logger
vi.mock('../../src/utils/logger.js', () => ({
	CursorDebugLogger: {
		info: vi.fn(),
		warn: vi.fn(),
		error: vi.fn(),
		debug: vi.fn(),
	},
}));

// Mock structured log provider
vi.mock('../../src/structuredLogProvider.js', () => ({
	StructuredLogProviderImpl: {
		info: vi.fn(),
		warn: vi.fn(),
		error: vi.fn(),
		debug: vi.fn(),
	},
}));

/**
 * Helper to create an async iterable that tracks how many items have been consumed
 */
function createTrackingAsyncIterable<T>(items: T[]): {
	iterable: AsyncIterable<T>;
	getConsumedCount: () => number;
} {
	let consumed = 0;
	return {
		iterable: (async function* () {
			for (const item of items) {
				consumed++;
				yield item;
			}
		})(),
		getConsumedCount: () => consumed,
	};
}

/**
 * Helper to create a simple async iterable from an array
 */
async function* createAsyncIterable<T>(items: T[]): AsyncIterable<T> {
	for (const item of items) {
		yield item;
	}
}

/**
 * Helper to collect all items from an async iterable
 */
async function collectAsyncIterable<T>(iterable: AsyncIterable<T>): Promise<T[]> {
	const results: T[] = [];
	for await (const item of iterable) {
		results.push(item);
	}
	return results;
}

/**
 * Helper to initialize a streaming request with retry configuration
 */
function initStreamingRequest(mockRequest: any, message?: AsyncIterable<any>): void {
	mockRequest.method.kind = MethodKind.BiDiStreaming;
	mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
	mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '2');
	mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
	mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');
	if (message !== undefined) {
		mockRequest.message = message;
	}
}


// Mock vscode.cursor.checkFeatureGate and getDynamicConfigValue
const mockCheckFeatureGate = vi.fn();
const mockGetDynamicConfigValue = vi.fn();
vi.mock('vscode', () => ({
	default: {
		cursor: {
			checkFeatureGate: (...args: any[]) => mockCheckFeatureGate(...args),
			getDynamicConfigValue: (...args: any[]) => mockGetDynamicConfigValue(...args),
		},
	},
	cursor: {
		checkFeatureGate: (...args: any[]) => mockCheckFeatureGate(...args),
		getDynamicConfigValue: (...args: any[]) => mockGetDynamicConfigValue(...args),
	},
}));

describe('RetryInterceptor', async () => {
	let mockRequest: any;
	let mockMethod: any;
	let mockService: any;

	beforeEach(() => {
		vi.clearAllMocks();
		vi.useFakeTimers();
		// Default to killswitch disabled (retry logic enabled)
		mockCheckFeatureGate.mockResolvedValue(false);
		// Default to fallback values from experimentConfig.ts
		mockGetDynamicConfigValue.mockResolvedValue({
			retriableErrors: [
				{ code: 'Unavailable' },
				{ code: 'Internal' },
				{ code: 'DeadlineExceeded' },
			],
		});

		// Create mock request structure
		mockMethod = {
			name: 'testMethod',
			kind: MethodKind.Unary,
		};

		mockService = {
			typeName: 'TestService',
		};

		mockRequest = {
			method: mockMethod,
			service: mockService,
			header: new Headers(),
		};
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	describe('executeWithRetry - Success scenarios', async () => {
		it('should succeed on first attempt without retries', async () => {
			// Enable retries via header
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');

			const interceptor = createUnaryRetryInterceptor();
			const next = vi.fn().mockResolvedValue({
				success: true
			});

			const result = await interceptor(next)(mockRequest);

			expect(next).toHaveBeenCalledTimes(1);
			expect(result).toEqual({
				success: true
			});
			expect(CursorDebugLogger.info).not.toHaveBeenCalled();
		});

		it('should succeed after retries', async () => {
			// Enable retries via header with custom config
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '3');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 3) {
					throw new ConnectError('Service unavailable', Code.Unavailable);
				}
				return Promise.resolve({
					success: true
				});
			});

			const resultPromise = interceptor(next)(mockRequest);

			// Fast-forward through retry delays
			await vi.advanceTimersByTimeAsync(100);

			const result = await resultPromise;

			expect(next).toHaveBeenCalledTimes(3);
			expect(result).toEqual({
				success: true
			});
			expect(CursorDebugLogger.info).toHaveBeenCalledWith(
				expect.stringContaining('Successfully completed after 2 retries (3 total requests made)')
			);
		});

		it('should succeed with streaming interceptor', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');

			mockService.typeName = 'aiserver.v1.ChatService';
			mockMethod.name = 'streamUnifiedChatWithTools';
			mockMethod.kind = MethodKind.BiDiStreaming;
			mockRequest.message = (async function* () {
				yield 'msg1';
			})();

			const interceptor = createStreamRetryInterceptor();
			const next = vi.fn().mockResolvedValue({
				stream: 'data',
			});

			const result = await interceptor(next)(mockRequest);

			expect(next).toHaveBeenCalledTimes(1);
			expect(result).toEqual({
				stream: 'data'
			});
		});
	});

	describe('executeWithRetry - Non-retryable errors', async () => {
		it('should not retry on non-ConnectError', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');

			const interceptor = createUnaryRetryInterceptor();
			const next = () => Promise.reject(new Error('Generic error'));

			await expect(interceptor(next)(mockRequest)).rejects.toThrow('Generic error');
		});

		it('should not retry on Canceled error code', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');

			const interceptor = createUnaryRetryInterceptor();
			const next = () => Promise.reject(new ConnectError('Canceled', Code.Canceled));

			await expect(interceptor(next)(mockRequest)).rejects.toThrow();
		});

		it('should not retry on InvalidArgument error code', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');

			const interceptor = createUnaryRetryInterceptor();
			const next = () => Promise.reject(new ConnectError('Invalid', Code.InvalidArgument));

			await expect(interceptor(next)(mockRequest)).rejects.toThrow();
		});
	});

	describe('executeWithRetry - Retryable errors', async () => {
		it('should retry on Unavailable error', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '2');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({
					success: true
				});
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(100);
			const result = await resultPromise;

			expect(next).toHaveBeenCalledTimes(2);
			expect(result).toEqual({
				success: true
			});
		});

		it('should retry on DeadlineExceeded error', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '2');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Deadline exceeded', Code.DeadlineExceeded);
				}
				return Promise.resolve({
					success: true
				});
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(100);
			const result = await resultPromise;

			expect(next).toHaveBeenCalledTimes(2);
			expect(result).toEqual({
				success: true
			});
		});

		it('should retry on Internal error', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '2');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Internal error', Code.Internal);
				}
				return Promise.resolve({
					success: true
				});
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(100);
			const result = await resultPromise;

			expect(next).toHaveBeenCalledTimes(2);
			expect(result).toEqual({
				success: true
			});
		});
	});

	describe('executeWithRetry - Max retries exhausted', async () => {
		it('should throw after max retries exhausted', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '2');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();

			const error = new ConnectError('Service unavailable', Code.Unavailable);
			const next = vi.fn().mockRejectedValue(error);

			let didThrow = false;
			interceptor(next)(mockRequest).catch((e) => {
				didThrow = true;
				expect(e).toBe(error);
			});
			await vi.advanceTimersByTimeAsync(200);

			expect(didThrow).toBe(true);
			expect(next).toHaveBeenCalledTimes(3); // initial + 2 retries
			expect(CursorDebugLogger.warn).toHaveBeenCalledWith(
				expect.stringContaining('Max retries (2) exhausted')
			);
		});

		it('should respect maxRetries: 0', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '0');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();

			const error = new ConnectError('Service unavailable', Code.Unavailable);
			const next = () => Promise.reject(error);

			let didThrow = false;
			interceptor(next)(mockRequest).catch((e) => {
				didThrow = true;
				expect(e).toBe(error);
			});
			await vi.advanceTimersByTimeAsync(50);

			expect(didThrow).toBe(true);
		});
	});

	describe('executeWithRetry - Exponential backoff with jitter', async () => {
		it('should apply exponential backoff', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '3');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '100');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '10000');

			const interceptor = createUnaryRetryInterceptor();

			const delays: number[] = [];
			let attemptCount = 0;

			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 4) {
					// Capture the delay before this attempt
					if (attemptCount > 1) {
						delays.push(Date.now());
					}
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({
					success: true
				});
			});

			const startTime = Date.now();
			const resultPromise = interceptor(next)(mockRequest);

			// Advance timers to trigger retries
			await vi.advanceTimersByTimeAsync(1000);

			await resultPromise;

			// Verify retries were attempted
			expect(next).toHaveBeenCalledTimes(4);
			// Verify backoff delays were applied (should be roughly exponential)
			expect(CursorDebugLogger.info).toHaveBeenCalledWith(
				expect.stringContaining('About to retry')
			);
		});

		it('should cap delay at maxDelayMs', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '5');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '1000');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 3) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({
					success: true
				});
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(300); // Should be enough with cap

			await resultPromise;

			expect(next).toHaveBeenCalledTimes(3);
		});
	});

	describe('Header validation', async () => {
		it('should reject non-numeric maxRetries header', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', 'not-a-number');

			const interceptor = createUnaryRetryInterceptor({
				maxRetries: 1,
				baseDelayMs: 10,
				maxDelayMs: 100,
			});

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({ success: true });
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(200);
			const result = await resultPromise;

			// Should use default maxRetries (1) instead of invalid header value
			expect(next).toHaveBeenCalledTimes(2); // 1 initial + 1 retry
			expect(result).toEqual({ success: true });
		});

		it('should reject negative maxRetries header', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '-1');

			const interceptor = createUnaryRetryInterceptor({
				maxRetries: 1,
				baseDelayMs: 10,
				maxDelayMs: 100,
			});

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({ success: true });
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(100);
			const result = await resultPromise;

			// Should use default maxRetries (1) instead of negative value
			expect(next).toHaveBeenCalledTimes(2); // 1 initial + 1 retry
			expect(result).toEqual({ success: true });
		});

		it('should accept maxRetries greater than 10', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '20');

			const interceptor = createUnaryRetryInterceptor({
				maxRetries: 1,
				baseDelayMs: 10,
				maxDelayMs: 100,
			});

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 3) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({ success: true });
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(200);
			const result = await resultPromise;

			// Should use header maxRetries (20) even though it's > 10
			expect(next).toHaveBeenCalledTimes(3); // 1 initial + 2 retries (of 20 possible)
			expect(result).toEqual({ success: true });
		});

		it('should reject baseDelayMs less than 1', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '0');

			const interceptor = createUnaryRetryInterceptor({
				maxRetries: 1,
				baseDelayMs: 100,
				maxDelayMs: 1000,
			});

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({ success: true });
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(200);
			const result = await resultPromise;

			// Should use default baseDelayMs (100) instead of 0
			expect(next).toHaveBeenCalledTimes(2);
			expect(result).toEqual({ success: true });
		});

		it('should accept maxDelayMs greater than 300000', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '600000');

			const interceptor = createUnaryRetryInterceptor({
				maxRetries: 1,
				baseDelayMs: 10,
				maxDelayMs: 100,
			});

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({ success: true });
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(700000); // Need to advance enough for 600000ms delay
			const result = await resultPromise;

			// Should use header maxDelayMs (600000) even though it's > 300000
			expect(next).toHaveBeenCalledTimes(2);
			expect(result).toEqual({ success: true });
		});
	});

	describe('Context-based configuration', async () => {
		it('should use header config when provided', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '1');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor({
				maxRetries: 5, // This should be overridden
				baseDelayMs: 1000,
				maxDelayMs: 5000,
			});

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({
					success: true
				});
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(50);

			const result = await resultPromise;

			expect(next).toHaveBeenCalledTimes(2);
			expect(result).toEqual({
				success: true
			});
		});

		it('should disable retries when header is not "true"', async () => {
			// Don't set X-Cursor-RetryInterceptor-Enabled header, or set it to false
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'false');

			const interceptor = createUnaryRetryInterceptor();
			const error = new ConnectError('Unavailable', Code.Unavailable);
			const next = () => Promise.reject(error);

			await expect(interceptor(next)(mockRequest)).rejects.toThrow();
		});

		it('should disable retries when no header provided', async () => {
			// Don't set any retry interceptor headers

			const interceptor = createUnaryRetryInterceptor();
			const error = new ConnectError('Unavailable', Code.Unavailable);
			const next = () => Promise.reject(error);

			await expect(interceptor(next)(mockRequest)).rejects.toThrow();
		});

		it('should use defaults when enabled: true without config', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');

			const interceptor = createUnaryRetryInterceptor({
				maxRetries: 2,
				baseDelayMs: 10,
				maxDelayMs: 100,
			});

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({
					success: true
				});
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(50);

			const result = await resultPromise;

			expect(next).toHaveBeenCalledTimes(2);
			expect(result).toEqual({
				success: true
			});
		});
	});

	describe('Method filtering', async () => {
		it('should only retry unary methods in unary interceptor', async () => {
			mockMethod.kind = MethodKind.ServerStreaming;

			const interceptor = createUnaryRetryInterceptor();
			const next = vi.fn().mockResolvedValue({
				success: true
			});

			await interceptor(next)(mockRequest);

			expect(next).toHaveBeenCalledTimes(1);
			// Should not log retry attempts since it's not a unary method
			expect(CursorDebugLogger.info).not.toHaveBeenCalled();
		});

		it('should only retry streaming methods in stream interceptor', async () => {
			mockService.typeName = 'aiserver.v1.ChatService';
			mockMethod.name = 'otherMethod';
			mockMethod.kind = MethodKind.BiDiStreaming;

			const interceptor = createStreamRetryInterceptor();
			const next = vi.fn().mockResolvedValue({
				success: true
			});

			await interceptor(next)(mockRequest);

			expect(next).toHaveBeenCalledTimes(1);
		});

		it('should handle ServerStreaming without wrapping message as AsyncIterable', async () => {
			// ServerStreaming: client sends a single message (not a stream), server streams back
			// The req.message should NOT be wrapped in RewindableAsyncIterable
			mockMethod.kind = MethodKind.ServerStreaming;
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '2');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			// Set message to a non-iterable object (like a regular protobuf message)
			const singleMessage = { content: 'test message' };
			mockRequest.message = singleMessage;

			const interceptor = createStreamRetryInterceptor();
			const next = vi.fn().mockResolvedValue({
				success: true
			});

			// This should not throw - if it tries to wrap the message in RewindableAsyncIterable,
			// it would fail because singleMessage is not an AsyncIterable
			const result = await interceptor(next)(mockRequest);

			expect(next).toHaveBeenCalledTimes(1);
			expect(result).toEqual({ success: true });
			// Verify the message was passed through unchanged (not wrapped)
			expect(next.mock.calls[0][0].message).toBe(singleMessage);
		});

		it('should handle ServerStreaming with retries without wrapping message', async () => {
			mockMethod.kind = MethodKind.ServerStreaming;
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '2');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			// Set message to a non-iterable object
			const singleMessage = { content: 'test message' };
			mockRequest.message = singleMessage;

			const interceptor = createStreamRetryInterceptor();
			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({ success: true });
			});

			const resultPromise = interceptor(next)(mockRequest);

			// Fast-forward through retry delays
			await vi.advanceTimersByTimeAsync(200);

			const result = await resultPromise;

			// Should have retried once
			expect(next).toHaveBeenCalledTimes(2);
			expect(result).toEqual({ success: true });
			// Both calls should have the original message (not wrapped)
			expect(next.mock.calls[0][0].message).toBe(singleMessage);
			expect(next.mock.calls[1][0].message).toBe(singleMessage);
		});
	});

	describe('Logging', async () => {
		it('should log retry attempts', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '2');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({
					success: true
				});
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(50);
			await resultPromise;

			expect(CursorDebugLogger.info).toHaveBeenCalledWith(
				expect.stringContaining('About to retry testMethod')
			);
			expect(StructuredLogProviderImpl.info).toHaveBeenCalledWith(
				'transport',
				'Retry attempt',
				expect.objectContaining({
					interceptor: 'UnaryRetryInterceptor',
					method: 'testMethod',
				})
			);
		});

		it('should log success after retries', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '2');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({
					success: true
				});
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(50);
			await resultPromise;

			expect(CursorDebugLogger.info).toHaveBeenCalledWith(
				expect.stringContaining('Successfully completed after 1 retries (2 total requests made)')
			);
			expect(StructuredLogProviderImpl.info).toHaveBeenCalledWith(
				'transport',
				'Retry succeeded',
				expect.objectContaining({
					interceptor: 'UnaryRetryInterceptor',
					method: 'testMethod',
					retryAttemptNumber: 1,
				})
			);
		});

		it('should log exhaustion when max retries reached', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '1');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();

			const error = new ConnectError('Unavailable', Code.Unavailable);
			const next = () => Promise.reject(error);

			let didThrow = false;
			interceptor(next)(mockRequest).catch((e) => {
				didThrow = true;
				expect(e).toBe(error);
			});
			await vi.advanceTimersByTimeAsync(50);

			expect(didThrow).to.equal(true);

			expect(CursorDebugLogger.warn).toHaveBeenCalledWith(
				expect.stringContaining('Max retries (1) exhausted')
			);
			expect(StructuredLogProviderImpl.warn).toHaveBeenCalledWith(
				'transport',
				'Retry exhausted',
				expect.objectContaining({
					interceptor: 'UnaryRetryInterceptor',
					method: 'testMethod',
					errorCode: 'Unavailable',
				})
			);
		});
	});

	describe('Edge cases', async () => {
		it('should handle rapid success after many failures', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '5');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '1');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '10');

			const interceptor = createUnaryRetryInterceptor();

			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 4) {
					throw new ConnectError('Unavailable', Code.Unavailable);
				}
				return Promise.resolve({
					success: true
				});
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(50);
			const result = await resultPromise;

			expect(next).toHaveBeenCalledTimes(4);
			expect(result).toEqual({
				success: true
			});
		});

		it('should handle synchronous errors', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '1');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();

			const error = new ConnectError('Unavailable', Code.Unavailable);
			const next = vi.fn().mockImplementation(() => {
				throw error;
			});

			let didThrow = false;
			interceptor(next)(mockRequest).catch((e) => {
				didThrow = true;
				expect(e).toBe(error);
			});

			await vi.advanceTimersByTimeAsync(50);

			expect(didThrow).toBe(true);
			expect(next).toHaveBeenCalledTimes(2); // initial + 1 retry
		});

		it('should work with different return types', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');

			const interceptor = createUnaryRetryInterceptor();
			const next = vi.fn().mockResolvedValue('string result');

			const result = await interceptor(next)(mockRequest);

			expect(result).toBe('string result');
		});
	});

	describe('Killswitch feature flag', async () => {
		it('should bypass unary retry interceptor when killswitch is enabled', async () => {
			// Enable killswitch
			mockCheckFeatureGate.mockResolvedValue(true);

			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '3');
			mockRequest.header.set('X-Cursor-RetryInterceptor-BaseDelayMs', '10');
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxDelayMs', '100');

			const interceptor = createUnaryRetryInterceptor();
			const expectedResult = {
				success: true
			};
			const next = vi.fn().mockResolvedValue(expectedResult);

			const result = await interceptor(next)(mockRequest);

			expect(result).toBe(expectedResult);
			expect(next).toHaveBeenCalledTimes(1); // Should only be called once, no retries
			expect(mockCheckFeatureGate).toHaveBeenCalledWith('retry_interceptor_disabled');
		});

		it('should bypass stream retry interceptor when killswitch is enabled', async () => {
			// Enable killswitch
			mockCheckFeatureGate.mockResolvedValue(true);

			initStreamingRequest(mockRequest);
			mockRequest.header.set('X-Cursor-RetryInterceptor-MaxRetries', '3');

			const interceptor = createStreamRetryInterceptor();
			const expectedResult = {
				success: true
			};
			const next = vi.fn().mockResolvedValue(expectedResult);

			const result = await interceptor(next)(mockRequest);

			expect(result).toBe(expectedResult);
			expect(next).toHaveBeenCalledTimes(1); // Should only be called once, no retries
			expect(mockCheckFeatureGate).toHaveBeenCalledWith('retry_interceptor_disabled');
		});
	});

	describe('calculateBackoffDelay', async () => {
		it('should apply exponential backoff with Full Jitter algorithm', () => {
			const baseDelayMs = 1000;
			const maxDelayMs = 30000;

			// Mock Math.random to return a fixed value for deterministic testing
			const originalRandom = Math.random;
			let randomValue = 0.5;
			Math.random = () => randomValue;

			try {
				// Attempt 0: baseDelay * 2^0 = 1000, capped at 30000, jittered
				const delay0 = calculateBackoffDelay(0, baseDelayMs, maxDelayMs);
				expect(delay0).toBe(500); // 0.5 * 1000

				// Attempt 1: baseDelay * 2^1 = 2000, capped at 30000, jittered
				randomValue = 0.75;
				const delay1 = calculateBackoffDelay(1, baseDelayMs, maxDelayMs);
				expect(delay1).toBe(1500); // 0.75 * 2000

				// Attempt 2: baseDelay * 2^2 = 4000, capped at 30000, jittered
				randomValue = 0.25;
				const delay2 = calculateBackoffDelay(2, baseDelayMs, maxDelayMs);
				expect(delay2).toBe(1000); // 0.25 * 4000

				// Attempt 10: baseDelay * 2^10 = 1024000, capped at 30000, jittered
				randomValue = 0.1;
				const delay10 = calculateBackoffDelay(10, baseDelayMs, maxDelayMs);
				expect(delay10).toBe(3000); // 0.1 * 30000 (capped)
			} finally {
				Math.random = originalRandom;
			}
		});

		it('should always return a value between 0 and the capped delay', () => {
			const baseDelayMs = 1000;
			const maxDelayMs = 30000;

			// Run multiple iterations to test randomness
			for (let attempt = 0; attempt < 5; attempt++) {
				for (let i = 0; i < 100; i++) {
					const delay = calculateBackoffDelay(attempt, baseDelayMs, maxDelayMs);
					const expectedMax = Math.min(baseDelayMs * Math.pow(2, attempt), maxDelayMs);
					expect(delay).toBeGreaterThanOrEqual(0);
					expect(delay).toBeLessThan(expectedMax);
				}
			}
		});

		it('should respect the max delay cap', () => {
			const baseDelayMs = 1000;
			const maxDelayMs = 5000;

			// Mock Math.random to return 1.0 (maximum jitter)
			const originalRandom = Math.random;
			Math.random = () => 1.0;

			try {
				// Attempt 10 would be 1024000 without cap, but should be capped at 5000
				const delay = calculateBackoffDelay(10, baseDelayMs, maxDelayMs);
				expect(delay).toBe(maxDelayMs);
			} finally {
				Math.random = originalRandom;
			}
		});

		it('should never exceed the max delay even with high exponential growth', () => {
			const baseDelayMs = 100;
			const maxDelayMs = 10000;

			// Mock Math.random to return 1.0 (maximum jitter)
			const originalRandom = Math.random;
			Math.random = () => 1.0;

			try {
				// Test many attempts with exponential growth
				for (let attempt = 0; attempt < 20; attempt++) {
					const delay = calculateBackoffDelay(attempt, baseDelayMs, maxDelayMs);
					expect(delay).toBeLessThanOrEqual(maxDelayMs);
				}
			} finally {
				Math.random = originalRandom;
			}
		});

		it('should produce varied delays with different random values', () => {
			const baseDelayMs = 1000;
			const maxDelayMs = 30000;
			const attempt = 2; // baseDelay * 2^2 = 4000

			const delays = new Set<number>();
			const iterations = 1000;

			// Collect delays from many random iterations
			for (let i = 0; i < iterations; i++) {
				const delay = calculateBackoffDelay(attempt, baseDelayMs, maxDelayMs);
				delays.add(Math.floor(delay / 100)); // Group into 100ms buckets
			}

			// With Full Jitter, we should see a wide distribution
			// The delays should be spread across the full range [0, 4000)
			expect(delays.size).toBeGreaterThan(10); // At least 10 different buckets
		});

		it('should handle zero attempt number correctly', () => {
			const baseDelayMs = 1000;
			const maxDelayMs = 30000;

			// Mock Math.random to return 0.5
			const originalRandom = Math.random;
			Math.random = () => 0.5;

			try {
				const delay = calculateBackoffDelay(0, baseDelayMs, maxDelayMs);
				expect(delay).toBe(500); // 0.5 * (1000 * 2^0) = 0.5 * 1000
			} finally {
				Math.random = originalRandom;
			}
		});

		it('should handle edge case where maxDelayMs equals baseDelayMs', () => {
			const baseDelayMs = 1000;
			const maxDelayMs = 1000;

			// Mock Math.random to return 0.5
			const originalRandom = Math.random;
			Math.random = () => 0.5;

			try {
				// Even attempt 0 should be capped at 1000
				const delay = calculateBackoffDelay(0, baseDelayMs, maxDelayMs);
				expect(delay).toBe(500); // 0.5 * 1000

				// Attempt 1 should also be capped at 1000
				const delay1 = calculateBackoffDelay(1, baseDelayMs, maxDelayMs);
				expect(delay1).toBe(500); // 0.5 * 1000 (capped, not 2000)
			} finally {
				Math.random = originalRandom;
			}
		});
	});

	describe('shouldRetryOnError - Dynamic config', async () => {
		it('should retry on default retriable errors when using fallback config', async () => {
			// Use fallback config with default retriable errors (Unavailable, Internal, DeadlineExceeded)
			const fallbackConfig = {
				retriableErrors: [
					{ code: 'Unavailable' },
					{ code: 'Internal' },
					{ code: 'DeadlineExceeded' },
				]
			};
			expect(shouldRetryOnError(Code.Unavailable, 'Service unavailable', 'testMethod', fallbackConfig)).toBe(true);
			expect(shouldRetryOnError(Code.DeadlineExceeded, 'Deadline exceeded', 'testMethod', fallbackConfig)).toBe(true);
			expect(shouldRetryOnError(Code.Internal, 'Internal error', 'testMethod', fallbackConfig)).toBe(true);
			expect(shouldRetryOnError(Code.NotFound, 'Not found', 'testMethod', fallbackConfig)).toBe(false);
		});

		it('should retry on code specified in dynamic config without errorMessage', async () => {
			const dynamicConfig = {
				retriableErrors: [
					{
						code: 'NotFound'
					},
				],
			};
			expect(shouldRetryOnError(Code.NotFound, 'Any error message', 'testMethod', dynamicConfig)).toBe(true);
		});

		it('should retry when errorMessage matches substring in dynamic config', async () => {
			const dynamicConfig = {
				retriableErrors: [
					{
						code: 'NotFound', errorMessage: 'connection reset'
					},
				],
			};
			expect(shouldRetryOnError(Code.NotFound, 'connection reset by peer', 'testMethod', dynamicConfig)).toBe(true);
			expect(shouldRetryOnError(Code.NotFound, 'The connection was reset', 'testMethod', dynamicConfig)).toBe(false);
			expect(shouldRetryOnError(Code.NotFound, 'Resource not found', 'testMethod', dynamicConfig)).toBe(false);
		});

		it('should not retry when errorMessage does not match substring', async () => {
			const dynamicConfig = {
				retriableErrors: [
					{
						code: 'NotFound', errorMessage: 'connection reset'
					},
				],
			};
			expect(shouldRetryOnError(Code.NotFound, 'Resource not found', 'testMethod', dynamicConfig)).toBe(false);
			expect(shouldRetryOnError(Code.NotFound, 'File not found', 'testMethod', dynamicConfig)).toBe(false);
		});

		it('should prioritize dynamic config over default retryable codes', async () => {
			const dynamicConfig = {
				retriableErrors: [
					{
						code: 'Unavailable', errorMessage: 'specific error'
					},
				],
			};
			// Should retry if errorMessage matches
			expect(shouldRetryOnError(Code.Unavailable, 'This is a specific error', 'testMethod', dynamicConfig)).toBe(true);
			// Should not retry if errorMessage does not match (even though Unavailable is default retryable)
			expect(shouldRetryOnError(Code.Unavailable, 'Generic unavailable error', 'testMethod', dynamicConfig)).toBe(false);
		});

		it('should handle multiple retriable error rules', async () => {
			const dynamicConfig = {
				retriableErrors: [
					{
						code: 'NotFound', errorMessage: 'timeout'
					},
					{
						code: 'Internal', errorMessage: 'rate limit'
					},
					{
						code: 'Unavailable'
					}, // No errorMessage, matches any
				],
			};
			expect(shouldRetryOnError(Code.NotFound, 'Request timeout', 'testMethod', dynamicConfig)).toBe(true);
			expect(shouldRetryOnError(Code.Internal, 'Rate limit exceeded', 'testMethod', dynamicConfig)).toBe(false);
			expect(shouldRetryOnError(Code.Internal, 'rate limit exceeded', 'testMethod', dynamicConfig)).toBe(true);
			expect(shouldRetryOnError(Code.Unavailable, 'Any message', 'testMethod', dynamicConfig)).toBe(true);
			expect(shouldRetryOnError(Code.NotFound, 'Not found', 'testMethod', dynamicConfig)).toBe(false);
		});

		it('should handle empty dynamic config gracefully', async () => {
			const dynamicConfig = {
				retriableErrors: []
			};
			// Should fall back to default retryable codes
			expect(shouldRetryOnError(Code.Unavailable, 'Service unavailable', 'testMethod', dynamicConfig)).toBe(false);
			expect(shouldRetryOnError(Code.NotFound, 'Not found', 'testMethod', dynamicConfig)).toBe(false);
		});

		it('should retry only when method matches when method is specified in config', async () => {
			const dynamicConfig = {
				retriableErrors: [
					{
						code: 'NotFound',
						method: 'streamUnifiedChatWithTools'
					},
				],
			};
			// Should retry when method matches
			expect(shouldRetryOnError(Code.NotFound, 'Any error', 'streamUnifiedChatWithTools', dynamicConfig)).toBe(true);
			// Should not retry when method does not match
			expect(shouldRetryOnError(Code.NotFound, 'Any error', 'otherMethod', dynamicConfig)).toBe(false);
		});

		it('should combine method and errorMessage filters', async () => {
			const dynamicConfig = {
				retriableErrors: [
					{
						code: 'NotFound',
						method: 'streamUnifiedChatWithTools',
						errorMessage: 'connection reset'
					},
				],
			};
			// Should retry when both method and errorMessage match
			expect(shouldRetryOnError(Code.NotFound, 'connection reset by peer', 'streamUnifiedChatWithTools', dynamicConfig)).toBe(true);
			// Should not retry when method doesn't match
			expect(shouldRetryOnError(Code.NotFound, 'connection reset by peer', 'otherMethod', dynamicConfig)).toBe(false);
			// Should not retry when errorMessage doesn't match
			expect(shouldRetryOnError(Code.NotFound, 'Resource not found', 'streamUnifiedChatWithTools', dynamicConfig)).toBe(false);
		});

		it('should handle multiple rules with different methods', async () => {
			const dynamicConfig = {
				retriableErrors: [
					{
						code: 'NotFound',
						method: 'streamUnifiedChatWithTools',
						errorMessage: 'timeout'
					},
					{
						code: 'NotFound',
						method: 'checkFeatureStatus',
						errorMessage: 'connection reset'
					},
				],
			};
			// Should match first rule
			expect(shouldRetryOnError(Code.NotFound, 'Request timeout', 'streamUnifiedChatWithTools', dynamicConfig)).toBe(true);
			// Should match second rule
			expect(shouldRetryOnError(Code.NotFound, 'connection reset', 'checkFeatureStatus', dynamicConfig)).toBe(true);
			// Should not match when method/errorMessage combination doesn't match any rule
			expect(shouldRetryOnError(Code.NotFound, 'Request timeout', 'checkFeatureStatus', dynamicConfig)).toBe(false);
			expect(shouldRetryOnError(Code.NotFound, 'connection reset', 'streamUnifiedChatWithTools', dynamicConfig)).toBe(false);
		});
	});

	describe('executeWithRetry - Dynamic config integration', async () => {
		it('should use dynamic config to retry on non-default retryable error', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');

			// Configure dynamic config to retry on NotFound errors
			mockGetDynamicConfigValue.mockResolvedValue({
				retriableErrors: [
					{
						code: 'NotFound', errorMessage: 'connection'
					},
				],
			});

			const interceptor = createUnaryRetryInterceptor({
				maxRetries: 1,
				baseDelayMs: 10,
				maxDelayMs: 100
			});
			let attemptCount = 0;
			const next = vi.fn().mockImplementation(async () => {
				attemptCount++;
				if (attemptCount === 1) {
					throw new ConnectError('connection reset', Code.NotFound);
				}
				return {
					success: true
				};
			});

			const resultPromise = interceptor(next)(mockRequest);

			// Fast-forward through retry delays
			await vi.advanceTimersByTimeAsync(200);

			const result = await resultPromise;

			expect(next).toHaveBeenCalledTimes(2);
			expect(result).toEqual({
				success: true
			});
			expect(CursorDebugLogger.info).toHaveBeenCalledWith(
				expect.stringContaining('About to retry')
			);
		});

		it('should not retry when errorMessage does not match dynamic config', async () => {
			mockRequest.header.set('X-Cursor-RetryInterceptor-Enabled', 'true');

			// Configure dynamic config to only retry on specific error message
			mockGetDynamicConfigValue.mockResolvedValue({
				retriableErrors: [
					{
						code: 'NotFound', errorMessage: 'connection reset'
					},
				],
			});

			const interceptor = createUnaryRetryInterceptor({
				maxRetries: 1,
				baseDelayMs: 10,
				maxDelayMs: 100
			});
			const next = vi.fn().mockRejectedValue(
				new ConnectError('Resource not found', Code.NotFound)
			);

			await expect(interceptor(next)(mockRequest)).rejects.toThrow(ConnectError);
			expect(next).toHaveBeenCalledTimes(1); // Should not retry
		});
	});

	describe('createStreamRetryInterceptor - Streaming retry', async () => {
		it('should retry streaming requests on Unavailable error and succeed after retries', async () => {
			initStreamingRequest(mockRequest, createAsyncIterable(['msg1', 'msg2']));

			const interceptor = createStreamRetryInterceptor();

			// Verify retry on Unavailable error and success after retries
			let attemptCount = 0;
			const next = vi.fn().mockImplementation(() => {
				attemptCount++;
				if (attemptCount < 2) {
					throw new ConnectError('Service unavailable', Code.Unavailable);
				}
				return Promise.resolve({
					message: createAsyncIterable(['response1', 'response2']),
				});
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(200);
			const result = await resultPromise;

			expect(next).toHaveBeenCalledTimes(2);
			expect(result.message).toBeDefined();
			// Consume the response to trigger success logging
			await collectAsyncIterable(result.message as AsyncIterable<string>);
			expect(CursorDebugLogger.info).toHaveBeenCalledWith(
				expect.stringContaining('Successfully completed after 1 retries')
			);
		});

		it('should not retry on non-ConnectError', async () => {
			initStreamingRequest(mockRequest, createAsyncIterable(['msg1']));

			const interceptor = createStreamRetryInterceptor();
			const next = vi.fn().mockRejectedValue(new Error('Generic error'));

			await expect(interceptor(next)(mockRequest)).rejects.toThrow('Generic error');
			expect(next).toHaveBeenCalledTimes(1); // Should not retry
		});

		it('should not retry on non-retryable codes (Canceled)', async () => {
			initStreamingRequest(mockRequest, createAsyncIterable(['msg1']));

			const interceptor = createStreamRetryInterceptor();
			const next = vi.fn().mockRejectedValue(new ConnectError('Canceled', Code.Canceled));

			await expect(interceptor(next)(mockRequest)).rejects.toThrow();
			expect(next).toHaveBeenCalledTimes(1); // Should not retry
		});

		it('should buffer request messages, replay on retry, and clear buffer on first response', async () => {
			initStreamingRequest(mockRequest);

			// Track how many times messages are consumed from source
			const requestMessages = ['msg1', 'msg2', 'msg3'];
			const tracker = createTrackingAsyncIterable(requestMessages);
			mockRequest.message = tracker.iterable;

			const interceptor = createStreamRetryInterceptor();

			// Track consumed messages per attempt
			const consumedPerAttempt: string[][] = [];
			let attemptCount = 0;

			const next = vi.fn().mockImplementation(async (req: any) => {
				attemptCount++;
				const consumed: string[] = [];

				// Consume messages from req.message (which is wrapped by RewindableAsyncIterable)
				for await (const msg of req.message) {
					consumed.push(msg as string);
					// On first attempt, fail after consuming 2 messages
					if (attemptCount === 1 && consumed.length === 2) {
						consumedPerAttempt.push(consumed);
						throw new ConnectError('Service unavailable', Code.Unavailable);
					}
				}
				consumedPerAttempt.push(consumed);

				return {
					message: createAsyncIterable(['response1', 'response2', 'response3']),
				};
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(200);
			const result = await resultPromise;

			// Verify retry occurred
			expect(next).toHaveBeenCalledTimes(2);

			// Verify first attempt consumed 2 messages before failing
			expect(consumedPerAttempt[0]).toEqual(['msg1', 'msg2']);

			// Verify second attempt got the buffered messages replayed plus continued from source
			// The RewindableAsyncIterable should replay msg1, msg2 from buffer, then continue with msg3 from source
			expect(consumedPerAttempt[1]).toEqual(['msg1', 'msg2', 'msg3']);

			// Verify source was only consumed once for msg1 and msg2 (buffered), then msg3 on retry
			// Total source consumption should be 3 (not 5 if it re-read everything)
			expect(tracker.getConsumedCount()).toBe(3);

			// Consume response events and verify buffer clearing
			const responses: string[] = [];
			for await (const event of result.message as AsyncIterable<string>) {
				responses.push(event);
			}
			expect(responses).toEqual(['response1', 'response2', 'response3']);
		});

		it('should exhaust retries for streaming', async () => {
			initStreamingRequest(mockRequest, createAsyncIterable(['msg1']));

			const interceptor = createStreamRetryInterceptor();

			// Verify max retries exhaustion
			const error = new ConnectError('Service unavailable', Code.Unavailable);
			const next1 = vi.fn().mockRejectedValue(error);

			let didThrow = false;
			interceptor(next1)(mockRequest).catch((e) => {
				didThrow = true;
				expect(e).toBe(error);
			});
			await vi.advanceTimersByTimeAsync(300);

			expect(didThrow).toBe(true);
			expect(next1).toHaveBeenCalledTimes(3); // 1 initial + 2 retries
			expect(CursorDebugLogger.warn).toHaveBeenCalledWith(
				expect.stringContaining('Max retries (2) exhausted')
			);
			expect(StructuredLogProviderImpl.warn).toHaveBeenCalledWith(
				'transport',
				'Retry exhausted',
				expect.objectContaining({
					interceptor: 'StreamRetryInterceptor',
					method: 'testMethod',
					errorCode: 'Unavailable',
				})
			);
		});

		it('should not retry when error message does not match dynamic config for streaming', async () => {
			initStreamingRequest(mockRequest, createAsyncIterable(['msg1']));

			const interceptor = createStreamRetryInterceptor();

			vi.clearAllMocks();
			mockGetDynamicConfigValue.mockResolvedValue({
				retriableErrors: [
					{ code: 'Unavailable', errorMessage: 'specific error' },
				],
			});
			mockRequest.message = createAsyncIterable(['msg1']);

			// Error message doesn't match config, should not retry
			const next = vi.fn().mockRejectedValue(
				new ConnectError('Generic unavailable error', Code.Unavailable)
			);

			await expect(interceptor(next)(mockRequest)).rejects.toThrow(ConnectError);
			expect(next).toHaveBeenCalledTimes(1); // Should not retry because message doesn't match
		});

		it('should retry when error message matches dynamic config for streaming', async () => {
			initStreamingRequest(mockRequest, createAsyncIterable(['msg1']));

			const interceptor = createStreamRetryInterceptor();

			vi.clearAllMocks();
			mockGetDynamicConfigValue.mockResolvedValue({
				retriableErrors: [
					{ code: 'Unavailable', errorMessage: 'specific error' },
				],
			});
			mockRequest.message = createAsyncIterable(['msg1']);

			let attempt = 0;
			const next = vi.fn().mockImplementation(() => {
				attempt++;
				if (attempt < 2) {
					throw new ConnectError('This is a specific error', Code.Unavailable);
				}
				return Promise.resolve({
					message: createAsyncIterable(['response']),
				});
			});

			const resultPromise = interceptor(next)(mockRequest);
			await vi.advanceTimersByTimeAsync(200);
			const result = await resultPromise;

			expect(next).toHaveBeenCalledTimes(2); // Should retry because message matches
			expect(result.message).toBeDefined();
			const responseMessages = await collectAsyncIterable(result.message as AsyncIterable<string>);
			expect(responseMessages).toEqual(['response']);
		});

		it('should not attempt more retries after receiving the first response event', async () => {
			initStreamingRequest(mockRequest, createAsyncIterable(['msg1']));

			const interceptor = createStreamRetryInterceptor();

			const errorCode = Code.Unavailable;
			const errorMessage = 'Stream failed mid-iteration';

			// First, verify that this error IS retryable according to the dynamic config
			// This ensures the test is actually testing "no retries after first response",
			// not just that the error happens to be non-retryable
			const isRetryable = shouldRetryOnError(
				errorCode,
				errorMessage,
				mockRequest.method.name,
				{ retriableErrors: [{ code: 'Unavailable' }, { code: 'Internal' }, { code: 'DeadlineExceeded' }] }
			);
			expect(isRetryable).toBe(true);

			// Create a response message that yields one event then throws a retryable error
			const createFailingResponseMessage = (): AsyncIterable<string> => ({
				[Symbol.asyncIterator](): AsyncIterator<string> {
					let yielded = false;
					return {
						async next(): Promise<IteratorResult<string>> {
							if (!yielded) {
								yielded = true;
								return { value: 'response1', done: false };
							}
							// Throw a retryable error after first event
							throw new ConnectError(errorMessage, errorCode);
						}
					};
				}
			});

			const next = vi.fn().mockResolvedValue({
				message: createFailingResponseMessage(),
			});

			const result = await interceptor(next)(mockRequest);

			// Start consuming the response - first event succeeds
			const responses: string[] = [];
			let caughtError: Error | undefined;
			try {
				for await (const event of result.message as AsyncIterable<string>) {
					responses.push(event);
				}
			} catch (error) {
				caughtError = error as Error;
			}

			// Should have received the first event before the error
			expect(responses).toEqual(['response1']);

			// Error should have been thrown during iteration
			expect(caughtError).toBeInstanceOf(ConnectError);
			expect((caughtError as ConnectError).code).toBe(errorCode);

			// Most importantly: next should only be called once - no retries after first response was received
			// Even though the error is retryable, we don't retry because we already received data
			expect(next).toHaveBeenCalledTimes(1);
		});
	});
});

describe('RewindableAsyncIterable', () => {
	it('should yield all items from source in order', async () => {
		const source: AsyncIterable<string> = createAsyncIterable(['a', 'b', 'c']);
		const buffering: RewindableAsyncIterable<string> = new RewindableAsyncIterable(source);

		const results: string[] = await collectAsyncIterable(buffering);

		expect(results).toEqual(['a', 'b', 'c']);
	});

	it('should handle empty source gracefully', async () => {
		const source: AsyncIterable<string> = createAsyncIterable([]);
		const buffering: RewindableAsyncIterable<string> = new RewindableAsyncIterable(source);

		const results: string[] = await collectAsyncIterable(buffering);

		expect(results).toEqual([]);
	});

	it('should allow rewinding by creating new iterators', async () => {
		const source: AsyncIterable<string> = createAsyncIterable(['a', 'b', 'c']);
		const buffering: RewindableAsyncIterable<string> = new RewindableAsyncIterable(source);

		const results1: string[] = await collectAsyncIterable(buffering);
		expect(results1).toEqual(['a', 'b', 'c']);

		const results2: string[] = await collectAsyncIterable(buffering);
		expect(results2).toEqual(['a', 'b', 'c']);
	});

	it('should maintain independent iterator positions', async () => {
		const source: AsyncIterable<string> = createAsyncIterable(['a', 'b', 'c', 'd']);
		const buffering: RewindableAsyncIterable<string> = new RewindableAsyncIterable(source);

		// Create two iterators
		const iterator1 = buffering[Symbol.asyncIterator]();
		const iterator2 = buffering[Symbol.asyncIterator]();

		// First iterator consumes 1 element
		const result1_1 = await iterator1.next();
		expect(result1_1.done).toBe(false);
		expect(result1_1.value).toBe('a');

		// Second iterator consumes 2 elements
		const result2_1 = await iterator2.next();
		expect(result2_1.done).toBe(false);
		expect(result2_1.value).toBe('a');

		const result2_2 = await iterator2.next();
		expect(result2_2.done).toBe(false);
		expect(result2_2.value).toBe('b');

		// First iterator consumes 1 more element
		// Should be done because a newer generation iterator has been created
		const result1_2 = await iterator1.next();
		expect(result1_2.done).toBe(true);

		// Second iterator consumes 1 elements
		const result2_3 = await iterator2.next();
		expect(result2_3.done).toBe(false);
		expect(result2_3.value).toBe('c');
	});

	it('should allow multiple iterators to block and receive buffered elements', async () => {
		// Create a controllable async iterable where we can control when items are yielded
		const pendingResolvers: Array<{
			resolve: (value: IteratorResult<string>) => void;
		}> = [];
		let nextCallCount = 0;

		const controllableSource: AsyncIterable<string> = {
			[Symbol.asyncIterator](): AsyncIterator<string> {
				return {
					async next(): Promise<IteratorResult<string>> {
						const callIndex = nextCallCount++;
						// Create a promise that will be resolved externally
						return new Promise<IteratorResult<string>>((resolve) => {
							pendingResolvers.push({ resolve });
						});
					},
				};
			},
		};

		const buffering: RewindableAsyncIterable<string> = new RewindableAsyncIterable(controllableSource);

		// Create two iterators
		const iterator1 = buffering[Symbol.asyncIterator]();
		const iterator2 = buffering[Symbol.asyncIterator]();

		// Make both iterators block on await this.sourceIterator.next()
		// Both will call next() when buffer is empty, so both will await sourceIterator.next()
		const next1Promise = iterator1.next();
		const next2Promise = iterator2.next();

		// Wait a bit to ensure both are blocked
		await new Promise((resolve) => setTimeout(resolve, 10));

		// Allow the sourceIterator to advance by 2 elements
		// Resolve the first next() call with element 'x'
		if (pendingResolvers.length > 0) {
			pendingResolvers[0].resolve({ done: false, value: 'x' });
		}
		// Resolve the second next() call with element 'y'
		if (pendingResolvers.length > 1) {
			pendingResolvers[1].resolve({ done: false, value: 'y' });
		}

		// Both iterators should produce these two elements
		// The first iterator will get 'x' first (it was the first to call next())
		// The second iterator will get 'x' from the buffer (since it was added before its next() resolved)
		const result1_1 = await next1Promise;
		expect(result1_1.done).toBe(true);

		const result2_1 = await next2Promise;
		expect(result2_1.done).toBe(false);
		expect(result2_1.value).toBe('x');

		const result2_2 = await iterator2.next();
		expect(result2_2.done).toBe(false);
		expect(result2_2.value).toBe('y');
	});

	// test multiple blocked iterators result in the last one returning events correctly
	// 1. create an iterable, advance it by 1
	// 2. make the source iterable block
	// 3. create another iterable, advance it by 2; now it should both emit the first value and be blocked on the second value
	// 4. without unblocking the source, create a third iterable, and advance it by 3
	// 5. finally, unblock the source completely
	// 6. now, we expect the second iterable to emit done; the thrid iterable successfully returns all three elements in order
	it('should handle multiple blocked iterators with generation invalidation', async () => {
		// Create a controllable async iterable where we can control when items are yielded
		const pendingResolvers: Array<{
			resolve: (value: IteratorResult<string>) => void;
		}> = [];

		const controllableSource: AsyncIterable<string> = {
			[Symbol.asyncIterator](): AsyncIterator<string> {
				return {
					async next(): Promise<IteratorResult<string>> {
						return new Promise<IteratorResult<string>>((resolve) => {
							pendingResolvers.push({ resolve });
						});
					},
				};
			},
		};

		const buffering: RewindableAsyncIterable<string> = new RewindableAsyncIterable(controllableSource);

		// Step 1: Create iterator1, advance by 1
		const iterator1 = buffering[Symbol.asyncIterator]();
		const next1_1Promise = iterator1.next();

		// Resolve first source call with 'a' - puts 'a' in the buffer
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(pendingResolvers.length).toBe(1);
		pendingResolvers[0].resolve({ done: false, value: 'a' });

		const result1_1 = await next1_1Promise;
		expect(result1_1.done).toBe(false);
		expect(result1_1.value).toBe('a');

		// Step 2: Make the source iterable block - iterator1 calls next() but we don't resolve
		const next1_2Promise = iterator1.next();
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(pendingResolvers.length).toBe(2);

		// Step 3: Create iterator2, advance by 2
		// First next() should return 'a' from buffer immediately
		// Second next() should block on source
		const iterator2 = buffering[Symbol.asyncIterator]();
		const next2_1Promise = iterator2.next();

		// This should resolve immediately from buffer
		const result2_1 = await next2_1Promise;
		expect(result2_1.done).toBe(false);
		expect(result2_1.value).toBe('a');

		// Second call to iterator2.next() - should block
		const next2_2Promise = iterator2.next();
		await new Promise((resolve) => setTimeout(resolve, 10));

		// Step 4: Create iterator3, advance by 3
		// First next() returns 'a' from buffer
		// Second and third next() calls will block
		const iterator3 = buffering[Symbol.asyncIterator]();
		const next3_1Promise = iterator3.next();

		// First call should return 'a' from buffer immediately
		const result3_1 = await next3_1Promise;
		expect(result3_1.done).toBe(false);
		expect(result3_1.value).toBe('a');

		// Second and third calls - will block
		const next3_2Promise = iterator3.next();
		const next3_3Promise = iterator3.next();
		await new Promise((resolve) => setTimeout(resolve, 10));

		// Step 5: Unblock the source completely
		// Resolve remaining pending resolvers with 'b', 'c', then done
		// pendingResolvers[0] was resolved with 'a'
		// pendingResolvers[1] is iterator1's blocked call
		// pendingResolvers[2] might be from iterator2 or iterator3
		// We need to resolve all pending ones

		// Resolve with 'b'
		if (pendingResolvers.length > 1) {
			pendingResolvers[1].resolve({ done: false, value: 'b' });
		}
		await new Promise((resolve) => setTimeout(resolve, 10));

		// Resolve with 'c'
		if (pendingResolvers.length > 2) {
			pendingResolvers[2].resolve({ done: false, value: 'c' });
		}
		await new Promise((resolve) => setTimeout(resolve, 10));

		// Resolve remaining with done
		for (let i = 3; i < pendingResolvers.length; i++) {
			pendingResolvers[i].resolve({ done: true, value: undefined as any });
		}

		// Step 6: Assert results
		// Iterator1 should be done (invalidated by iterator2's generation)
		const result1_2 = await next1_2Promise;
		expect(result1_2.done).toBe(true);

		// Iterator2 should be done (invalidated by iterator3's generation)
		const result2_2 = await next2_2Promise;
		expect(result2_2.done).toBe(true);

		// Iterator3 should receive 'b' and 'c' (already got 'a')
		const result3_2 = await next3_2Promise;
		expect(result3_2.done).toBe(false);
		expect(result3_2.value).toBe('b');

		const result3_3 = await next3_3Promise;
		expect(result3_3.done).toBe(false);
		expect(result3_3.value).toBe('c');
	});

	// test onRetryFinished does not affect iterator outputs
	// 1. create an iterator, advance by 1
	// 2. make source iterator block
	// 3. create another iterator, advance by 2
	// 4. without unblocking, create yet another iterator and advance by 3
	// 5. unblock the source iterator by 1
	// 6. we expect the second iterator to be unblocked, however, it should only emit done because it's not the latest generation anymore
	// 7. call retryFinished
	// 8. unblock the source completely
	// 9. we expect the third (final iterator) to return 3 elements in order
	// 10. we can continue adding elements to the source iterator, and the final iterator returns the new elements as well
	// 11. finally, creating a new iterator after calling retryFinished results in an exception being thrown
	it('should handle generation invalidation with retryFinished and continued element consumption', async () => {
		// Create a controllable async iterable where we can control when items are yielded
		const pendingResolvers: Array<{
			resolve: (value: IteratorResult<string>) => void;
		}> = [];

		const controllableSource: AsyncIterable<string> = {
			[Symbol.asyncIterator](): AsyncIterator<string> {
				return {
					async next(): Promise<IteratorResult<string>> {
						return new Promise<IteratorResult<string>>((resolve) => {
							pendingResolvers.push({ resolve });
						});
					},
				};
			},
		};

		const buffering: RewindableAsyncIterable<string> = new RewindableAsyncIterable(controllableSource);

		// Step 1: Create iterator1, advance by 1
		const iterator1 = buffering[Symbol.asyncIterator]();
		const next1_1Promise = iterator1.next();

		// Resolve first source call with 'a' - puts 'a' in the buffer
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(pendingResolvers.length).toBe(1);
		pendingResolvers[0].resolve({ done: false, value: 'a' });

		const result1_1 = await next1_1Promise;
		expect(result1_1.done).toBe(false);
		expect(result1_1.value).toBe('a');

		// Step 2: Make source iterator block - iterator1 calls next() but we don't resolve
		const next1_2Promise = iterator1.next();
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(pendingResolvers.length).toBe(2);

		// Step 3: Create iterator2, advance by 2
		// First next() should return 'a' from buffer immediately
		const iterator2 = buffering[Symbol.asyncIterator]();
		const next2_1Promise = iterator2.next();

		// This should resolve immediately from buffer
		const result2_1 = await next2_1Promise;
		expect(result2_1.done).toBe(false);
		expect(result2_1.value).toBe('a');

		// Second call to iterator2.next() - should block on source
		const next2_2Promise = iterator2.next();
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(pendingResolvers.length).toBe(3); // iterator2 created a new pending resolver

		// Step 4: Without unblocking, create iterator3 and advance by 3
		const iterator3 = buffering[Symbol.asyncIterator]();

		// First next() returns 'a' from buffer immediately
		const next3_1Promise = iterator3.next();
		const result3_1 = await next3_1Promise;
		expect(result3_1.done).toBe(false);
		expect(result3_1.value).toBe('a');

		// Second and third calls - will block (third queues behind second)
		const next3_2Promise = iterator3.next();
		const next3_3Promise = iterator3.next(); // this one doesn't create a new pending resolver because the previous one is still pending
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(pendingResolvers.length).toBe(4); // iterator3 created a new pending resolver

		// Step 5: Unblock the source iterator by 1 (resolve 'b' for iterator1's blocked call)
		// pendingResolvers[1] is iterator1's second next() call
		pendingResolvers[1].resolve({ done: false, value: 'b' });
		await new Promise((resolve) => setTimeout(resolve, 10));

		// iterator1 adds 'b' to buffer (not latest gen), then exits
		const result1_2 = await next1_2Promise;
		expect(result1_2.done).toBe(true);

		// Step 6: Unblock iterator2's blocked call - it should also emit done
		// because iterator3 was created after it (generation invalidated)
		// pendingResolvers[2] is iterator2's second next() call
		pendingResolvers[2].resolve({ done: false, value: 'c' });
		await new Promise((resolve) => setTimeout(resolve, 10));

		const result2_2 = await next2_2Promise;
		expect(result2_2.done).toBe(true);

		// Now buffer should be ['a', 'b', 'c']

		// Step 7: Call retryFinished
		buffering.onRetryFinished();

		// Step 8: Unblock the source completely for iterator3
		// pendingResolvers[3] is iterator3's second next() call
		pendingResolvers[3].resolve({ done: false, value: 'd' });
		await new Promise((resolve) => setTimeout(resolve, 10));

		// Step 9: Assert iterator3 returns elements in order
		// next3_2Promise should get 'b' (from buffer, since 'b' was added by iterator1)
		const result3_2 = await next3_2Promise;
		expect(result3_2.done).toBe(false);
		expect(result3_2.value).toBe('b');

		// next3_3Promise should get 'c' (from buffer, since 'c' was added by iterator2)
		const result3_3 = await next3_3Promise;
		expect(result3_3.done).toBe(false);
		expect(result3_3.value).toBe('c');

		// Step 10: Continue advancing iterator3 to receive 'd' and more
		const next3_4Promise = iterator3.next();
		const result3_4 = await next3_4Promise;
		expect(result3_4.done).toBe(false);
		expect(result3_4.value).toBe('d');

		// Add another element to show we can continue after retryFinished
		const next3_5Promise = iterator3.next();
		await new Promise((resolve) => setTimeout(resolve, 10));
		// Resolve with 'e'
		pendingResolvers[pendingResolvers.length - 1].resolve({ done: false, value: 'e' });

		const result3_5 = await next3_5Promise;
		expect(result3_5.done).toBe(false);
		expect(result3_5.value).toBe('e');

		// Exhaust iterator3 to completion
		const next3_6Promise = iterator3.next();
		await new Promise((resolve) => setTimeout(resolve, 10));
		pendingResolvers[pendingResolvers.length - 1].resolve({ done: true, value: undefined as any });
		const result3_6 = await next3_6Promise;
		expect(result3_6.done).toBe(true);

		// Step 11: Creating a new iterator after calling retryFinished results in an exception
		// Note: Generator functions don't execute until .next() is called
		const newIterator = buffering[Symbol.asyncIterator]();
		await expect(newIterator.next()).rejects.toThrow('No new iterators can be created after retry finished; this is a bug');
	});
});

