/**
 * Base PlaywrightCrawler configuration shared across all browser scrapers.
 *
 * Provides:
 * - Standard crawler setup with configurable concurrency
 * - Integration with SQLiteStore for item persistence
 * - Anti-bot defaults via getStealthLaunchOptions()
 * - CLI argument parsing for --run-id, --db-path, --headless
 *
 * Usage:
 *   import { createBaseCrawler, parseCLIArgs } from './base-crawler';
 *   const config = parseCLIArgs();
 *   const crawler = createBaseCrawler(config, { requestHandler: ... });
 *   await crawler.run([startUrl]);
 */

import { PlaywrightCrawler, PlaywrightCrawlerOptions, ProxyConfiguration } from 'crawlee';
import { getStealthLaunchOptions } from './anti-bot';
import { SQLiteStore } from './sqlite-store';

export interface CrawlerConfig {
    country: string;
    dbPath: string;
    runId: string;
    maxConcurrency: number;
    headless: boolean;
    maxRequestRetries: number;
    requestHandlerTimeoutSecs: number;
}

const DEFAULTS: Partial<CrawlerConfig> = {
    maxConcurrency: 5,
    headless: true,
    maxRequestRetries: 3,
    requestHandlerTimeoutSecs: 120,
};

/**
 * Parse CLI arguments passed from Python's subprocess call.
 * Expected: node scraper.js --country X --db-path Y --run-id Z [--headless] [--max-concurrency N]
 */
export function parseCLIArgs(): CrawlerConfig {
    const args = process.argv.slice(2);
    const get = (flag: string, fallback: string = ''): string => {
        const idx = args.indexOf(flag);
        return idx >= 0 && idx + 1 < args.length ? args[idx + 1] : fallback;
    };
    const has = (flag: string): boolean => args.includes(flag);

    return {
        country: get('--country', 'unknown'),
        dbPath: get('--db-path', ''),
        runId: get('--run-id', `run_${Date.now()}`),
        maxConcurrency: parseInt(get('--max-concurrency', '5'), 10),
        headless: !has('--no-headless'),
        maxRequestRetries: parseInt(get('--max-retries', '3'), 10),
        requestHandlerTimeoutSecs: parseInt(get('--timeout', '120'), 10),
    };
}

/**
 * Create a PlaywrightCrawler with standard platform configuration.
 *
 * @param config - Crawler configuration from CLI or programmatic setup.
 * @param options - Additional PlaywrightCrawlerOptions (must include requestHandler).
 * @returns Configured PlaywrightCrawler instance.
 */
export function createBaseCrawler(
    config: CrawlerConfig,
    options: Partial<PlaywrightCrawlerOptions>,
): PlaywrightCrawler {
    const stealthOptions = getStealthLaunchOptions();

    const crawler = new PlaywrightCrawler({
        maxConcurrency: config.maxConcurrency,
        maxRequestRetries: config.maxRequestRetries,
        requestHandlerTimeoutSecs: config.requestHandlerTimeoutSecs,
        headless: config.headless,

        launchContext: {
            launchOptions: {
                ...stealthOptions,
                headless: config.headless,
            },
        },

        // Session pool for anti-bot rotation
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: config.maxConcurrency * 2,
            sessionOptions: {
                maxUsageCount: 50,
            },
        },

        // Merge caller-provided options (requestHandler, etc.)
        ...options,
    });

    return crawler;
}

/**
 * Create an SQLiteStore for the given config.
 * Convenience wrapper for use in requestHandler closures.
 */
export function createStore(config: CrawlerConfig): SQLiteStore {
    if (!config.dbPath) {
        throw new Error('--db-path is required');
    }
    return new SQLiteStore(config.dbPath, config.runId);
}
