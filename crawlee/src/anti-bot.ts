/**
 * Anti-bot and stealth configuration for Playwright in Crawlee.
 *
 * Mirrors the Python core/stealth_profile.py and core/human_actions.py logic
 * but leverages Crawlee's built-in fingerprinting and session rotation.
 *
 * Provides:
 * - Stealth launch options (disable automation signals)
 * - Human-like delays and scroll behavior
 * - Viewport randomization
 */

import type { LaunchOptions } from 'playwright';

/** Random integer in [min, max]. */
function randInt(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Get Playwright launch options that reduce detection:
 * - Disables blink automation features
 * - Randomizes viewport
 * - Sets realistic user agent args
 */
export function getStealthLaunchOptions(): LaunchOptions {
    return {
        args: [
            '--disable-blink-features=AutomationControlled',
            '--disable-features=IsolateOrigins,site-per-process',
            '--disable-infobars',
            '--no-first-run',
            '--no-default-browser-check',
            `--window-size=${randInt(1200, 1920)},${randInt(800, 1080)}`,
        ],
        ignoreDefaultArgs: ['--enable-automation'],
    };
}

/**
 * Wait for a random duration to mimic human behavior.
 * @param minMs - Minimum delay in milliseconds.
 * @param maxMs - Maximum delay in milliseconds.
 */
export async function randomDelay(minMs: number = 500, maxMs: number = 2000): Promise<void> {
    const delay = randInt(minMs, maxMs);
    await new Promise((resolve) => setTimeout(resolve, delay));
}

/**
 * Perform human-like scrolling on a page.
 * Scrolls down in random increments with random pauses.
 * @param page - Playwright Page instance.
 * @param scrolls - Number of scroll actions (default 3-6).
 */
export async function humanScroll(page: any, scrolls?: number): Promise<void> {
    const count = scrolls ?? randInt(3, 6);
    for (let i = 0; i < count; i++) {
        const distance = randInt(100, 500);
        await page.mouse.wheel(0, distance);
        await randomDelay(300, 800);
    }
}

/**
 * Move mouse to a random position on the page.
 * Helps avoid "no mouse movement" detection.
 * @param page - Playwright Page instance.
 */
export async function randomMouseMove(page: any): Promise<void> {
    const x = randInt(100, 800);
    const y = randInt(100, 600);
    await page.mouse.move(x, y, { steps: randInt(5, 15) });
    await randomDelay(100, 300);
}
