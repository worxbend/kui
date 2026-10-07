/**
 * Run axe over every Storybook story, in both themes, and fail if anything is wrong.
 *
 * ## Why this exists when the a11y addon is already installed
 *
 * The addon checks the story you are looking at. That is the right tool while building a component
 * and the wrong one for keeping a workspace correct: nobody clicks two hundred stories, and the two
 * defects this script found on its first run — a contrast failure on `StatTile`'s "not measured"
 * text, and six paginators producing six identically named landmarks — were both in stories that
 * had been reviewed by eye and passed.
 *
 * It also checks something the addon cannot: **both themes**. The palette is two palettes, contrast
 * is a property of a pair of colours, and a component can be legible in dark and fail in light.
 * Every story here is rendered twice.
 *
 * ## Why `region` is disabled
 *
 * A story root is not inside a `<main>`, so `region` fails for every story in the workspace. That
 * is the harness, not the component — in the product these all render inside the frame's `<main>`.
 * It is the only rule turned off, and turning off a second one needs a reason written here.
 *
 * ## Usage: three commands, and the first two are not optional
 *
 *   pnpm build-storybook
 *   pnpm exec http-server storybook-static -p 6017 -s &     # pinned server, as in CI
 *   pnpm a11y                                                # or: node scripts/a11y-stories.mjs
 *   node scripts/a11y-stories.mjs 'chrome-|surfaces-'        # only matching story ids
 *
 * The default origin is `http://localhost:6017`, which is the **built** Storybook served as static
 * files — the shape CI runs and the shape the sweep is calibrated against. `pnpm storybook` is the
 * dev server and listens on **6006**, so pointing this at it needs `SB=http://localhost:6006` and
 * gets a different thing measured: stories are compiled on demand, so the first visit to a story
 * competes with its own build and the theme wait below is doing two jobs at once. This block used
 * to say `pnpm storybook` and nothing else, on a port the script has never read, so the documented
 * invocation exited 2 with "Could not reach Storybook" — the message is right and the instruction
 * above it was wrong.
 *
 * `SB` overrides the origin.
 *
 * ## Exit codes, because two very different things used to look the same
 *
 *   0 — every story rendered in both themes and axe found nothing.
 *   1 — axe found violations. They are printed above the summary, one block each.
 *   2 — **the sweep could not run**: Storybook was unreachable, the filter matched no stories, an
 *       unfiltered sweep found too few stories to be reading a whole workspace, or a story would not
 *       take the theme it was asked for. Nothing is being said about accessibility in this case, and
 *       the message says so in as many words — the previous version printed the theme failure with
 *       the same `✗ <story id>` prefix a violation uses, and two wave-4 packets read a loaded
 *       machine as an a11y regression because of it.
 *
 * A story whose theme lands *after* the last budget expires is **not** exit 2. The attribute is
 * read back before the failure is declared, and when it turns out to be right the story is swept
 * normally with a note saying how long it took — see the block below the retry loop.
 */
import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { cpus, loadavg } from "node:os";
import { chromium } from "playwright";

const require = createRequire(import.meta.url);
const axeSource = readFileSync(require.resolve("axe-core/axe.min.js"), "utf8");

const base = process.env.SB ?? "http://localhost:6017";
const filter = process.argv[2] === undefined ? undefined : new RegExp(process.argv[2]);

/** The harness's own failure, not a component's. See the header. */
const DISABLED_RULES = { region: { enabled: false } };

/**
 * How long to wait for `.storybook/preview.tsx` to stamp `data-theme` on the root, per attempt.
 *
 * A widening budget rather than two attempts at a flat ten seconds. Wave 4 measured the flat wait
 * failing on an arbitrary story — a different one in each of two reports, each passing when run
 * alone, with **no axe violation printed in any run** — under a load average of 21 on 16 cores,
 * while the same sweep was clean twice at a normal load. So it is the story's render losing to the
 * machine rather than anything about the story, and a budget that does not widen under load fails
 * CI for a reason that has nothing to do with accessibility.
 *
 * Three attempts, each a fresh navigation: the second and third also serve the original purpose of
 * the retry, which is a feature chunk that had not been built yet on the first visit and is warm on
 * the second. The totals matter more than the individual numbers — a story that has not themed
 * after seventy seconds of waiting across three loads, *on an unloaded machine*, is not slow, it is
 * broken, and the sweep should say so rather than wait for ever. That qualification is new and is
 * the whole of the block below: these are the budgets at a load average of one core per core.
 */
const THEME_BUDGETS_MS = [10_000, 20_000, 40_000];

/**
 * How much wider those budgets have to be on the machine this run is actually on.
 *
 * The numbers above were calibrated in wave 4 against a load average of 21 on 16 cores. Wave 8 runs
 * thirteen packets at once: measured here at **load 45 on 16 cores, with four other copies of this
 * sweep in flight**, two consecutive whole-workspace runs died at the third budget on two
 * *different* stories — `lists-pagination--the-extremes` and `charts-barchart--all-zero` — and each
 * of them themed and swept clean in both themes when re-run on its own seconds later. Nothing about
 * either story is slow; what is slow is a chromium page load competing with four other browsers and
 * two test runners for sixteen cores.
 *
 * So the budget stops being a fixed number of seconds and becomes a number of seconds *per unit of
 * contention*, which is what it was always trying to be. The factor is the one-minute load average
 * over the core count, rounded, floored at 1 and capped at 4 — the cap is what keeps "not slow,
 * broken" a reachable verdict, because without one a genuinely broken story would hold the sweep for
 * as long as the machine was busy, which on a wave day is for ever.
 *
 * This is the same defect as `vitest.config.ts`'s `testTimeout`, in a second harness: a wall-clock
 * deadline measuring work done on a shared CPU tells you about the machine, not about the thing
 * under test. Both were found the same afternoon and neither is a story's or a case's fault.
 */
const CONTENTION = Math.min(4, Math.max(1, Math.round(loadavg()[0] / cpus().length)));

const THEME_WAIT_MS = THEME_BUDGETS_MS.map((budget) => budget * CONTENTION);

const index = await fetch(`${base}/index.json`)
  .then((response) => response.json())
  .catch(() => {
    console.error(`Could not reach Storybook at ${base}. Start it with \`pnpm storybook\`.`);
    process.exit(2);
  });

const ids = Object.values(index.entries)
  .filter((entry) => entry.type === "story")
  .map((entry) => entry.id)
  .filter((id) => filter === undefined || filter.test(id));

if (ids.length === 0) {
  console.error("No stories matched.");
  process.exit(2);
}

/**
 * The floor an unfiltered sweep has to clear, and why a floor at all.
 *
 * `storybook-static/` is a **shared output directory** and a failed build leaves it half-written.
 * The sweep then reads whatever `index.json` survived, checks the handful of stories it names, and
 * prints `✓ N stories × 2 themes: no violations` — a pass it did not earn, over a workspace it
 * mostly did not look at. That happened twice in wave 7 and nothing here could tell: every other
 * harness failure in this script is loud, and this one is silent by construction because a shorter
 * roster is not an error, it is just a smaller number in a line nobody compares to anything.
 *
 * So the roster gets the same treatment `CssReferencesSuite`'s *"there are stylesheets to check"*
 * gives its own walk. The build at wave 8 emits **780** stories; the floor is set well below that on
 * purpose, so that adding or retiring a handful is not a two-file change, while a collapse to a
 * fragment of the workspace is impossible to miss. It applies only to an unfiltered sweep, because a
 * filter is a deliberate request for a subset — which is also the cheapest way to defeat this guard,
 * and is disclosed rather than defended: `node scripts/a11y-stories.mjs '.'` passes a filter that
 * matches everything and skips the floor. Deleting the four lines below is cheaper still.
 */
const ROSTER_FLOOR = 700;

if (filter === undefined && ids.length < ROSTER_FLOOR) {
  console.error(`\nHARNESS FAILURE — nothing was checked, and this is not a clean sweep.`);
  console.error(`  ${base}/index.json names ${ids.length} stories; a whole-workspace sweep expects`);
  console.error(`  at least ${ROSTER_FLOOR}. The usual cause is a half-written storybook-static/`);
  console.error(`  left by a build that failed, which is a shared directory between packets.`);
  console.error(`  Rebuild before reporting anything about accessibility:`);
  console.error(`      pnpm -C frontend build-storybook`);
  process.exit(2);
}

// Said out loud at the top of every run, because the next reader of a harness failure below needs to
// know which budget it was measured against — a report quoting "70s" from a run that actually waited
// 210 is the sort of thing that gets the wrong thing fixed.
console.log(
  `Sweeping ${ids.length} stories in 2 themes. Load ${loadavg()[0].toFixed(1)} over ` +
    `${cpus().length} cores, so the theme budgets are ${THEME_WAIT_MS.map((ms) => ms / 1000).join(
      "/",
    )}s (×${CONTENTION}).`,
);

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });

let failures = 0;
for (const theme of ["dark", "light"]) {
  for (const id of ids) {
    // The theme goes through Storybook's `theme` global, not by setting `data-theme` directly.
    // `.storybook/preview.tsx` owns that attribute and rewrites it from the global on every story
    // render, so an attribute set from here is silently reverted — which is exactly what happened
    // on this script's first version: both passes rendered dark and the second was *labelled*
    // light. A check that quietly tests the same thing twice is worse than no check, so the wait
    // below is on the attribute itself.
    const url = `${base}/iframe.html?id=${id}&viewMode=story&globals=theme:${theme}`;
    let themed = false;
    let waitedMs = 0;
    // See `THEME_WAIT_MS`: each attempt is a fresh navigation on a wider budget, so a slow render
    // under load is told apart from a story that genuinely never applies the theme.
    for (const budget of THEME_WAIT_MS) {
      await page.goto(url, { waitUntil: "load" });
      const startedAt = Date.now();
      try {
        await page.waitForFunction(
          (expected) => document.documentElement.getAttribute("data-theme") === expected,
          theme,
          { timeout: budget },
        );
        themed = true;
      } catch {
        /* Try again, from a warm cache and with more room. */
      }
      waitedMs += Date.now() - startedAt;
      if (themed) break;
    }

    if (!themed) {
      /*
       * Read the attribute back before calling this a failure, because the last budget expiring is
       * not the same fact as the theme never arriving.
       *
       * `waitForFunction` polls, so it can time out in the gap between the attribute landing and
       * the next poll — and under the load this retry exists for, that gap is exactly where a slow
       * render finishes. The previous version went straight to the message below and printed
       * `asked for the dark theme and got dark`, which is self-contradictory in precisely the case
       * the whole retry was written for: two wave-4 packets read a loaded machine as an a11y
       * regression, and this line was the reason the third reader could not tell which it was.
       *
       * When it did arrive, the story is themed and axe below is measuring the right palette, so
       * the sweep continues and says the wait was long rather than failing a CI run over it.
       */
      const applied = await page.evaluate(() => document.documentElement.getAttribute("data-theme"));
      const seconds = Math.round(waitedMs / 1000);
      if (applied === theme) {
        console.error(`  ${id}: the ${theme} theme landed after the last budget expired — ${seconds}s`);
        console.error(`  of waiting across ${THEME_WAIT_MS.length} navigations. Checked anyway: the`);
        console.error(`  attribute is right, so what axe measures below is the right palette.`);
      } else {
        // Deliberately not the `✗ <id>` shape a violation is printed with. This story was never
        // checked, so the run has found nothing about it either way, and the two must not read alike.
        console.error(`\nHARNESS FAILURE — nothing was checked here, and this is not a violation.`);
        console.error(`  ${id}: asked for the ${theme} theme and got ${applied ?? "none"}, after`);
        console.error(`  ${THEME_WAIT_MS.length} navigations and ${seconds}s of waiting in total,`);
        console.error(`  on budgets already widened ×${CONTENTION} for the load this run started at.`);
        console.error(`  A theme that never applied means the sweep would check one theme twice,`);
        console.error(`  so it stops here rather than report a pass it did not earn.`);
        console.error(`  Re-run this story on its own before reporting a defect:`);
        console.error(`      node scripts/a11y-stories.mjs '^${id}$'`);
        await browser.close();
        process.exit(2);
      }
    }

    /*
     * Wait for the entry animations to finish before measuring anything.
     *
     * Contrast is computed from what is actually painted, and a dialog or a drawer fades in — so a
     * sweep that ran the instant the story loaded measured *semi-transparent* text against the
     * surface behind it and reported a contrast failure. The tell was that the same element failed
     * with a different ratio on every run: 4.34, then 3.48, then 3.49. A real contrast failure is
     * the same number every time.
     *
     * It also made the count depend on how many dialog stories the workspace happened to contain,
     * which is the worst property a regression check can have — adding a story to a component that
     * was already correct made the number go up.
     *
     * `getAnimations` covers both CSS transitions and animations, and the timeout is a bound rather
     * than a wait: a story with an intentionally infinite animation (a spinner) would otherwise
     * hang the sweep for ever.
     */
    await page
      .waitForFunction(
        () => document.getAnimations().every((animation) => animation.playState !== "running"),
        undefined,
        { timeout: 2_000 },
      )
      .catch(() => {
        /* A story with a looping animation — a spinner — never settles. Measure it anyway. */
      });

    // The a11y addon already puts an axe on `window`, and it runs itself on every story render.
    // Injecting a second copy gives two axes sharing one internal lock, and the sweep dies partway
    // through with "Axe is already running". So: reuse the addon's instance when it is there, and
    // wait for its automatic run to finish rather than racing it.
    const hasAxe = await page.evaluate(() => typeof window.axe === "object");
    if (!hasAxe) await page.addScriptTag({ content: axeSource });

    const result = await page.evaluate(
      async (rules) => {
        const options = { resultTypes: ["violations"], rules };
        for (let attempt = 0; ; attempt++) {
          try {
            return await window.axe.run(document.body, options);
          } catch (error) {
            const busy = String(error).includes("already running");
            if (!busy || attempt >= 20) throw error;
            await new Promise((resolve) => setTimeout(resolve, 100));
          }
        }
      },
      DISABLED_RULES,
    );

    for (const violation of result.violations) {
      failures++;
      console.log(`\n✗ [${theme}] ${id}`);
      console.log(`  ${violation.id} (${violation.impact}): ${violation.help}`);
      for (const node of violation.nodes.slice(0, 3)) {
        console.log(`    ${node.target.join(" ")}`);
        const summary = (node.failureSummary ?? "").split("\n").filter(Boolean).at(-1);
        if (summary !== undefined) console.log(`    ${summary.trim()}`);
      }
    }
  }
}

await browser.close();

const checked = `${ids.length} stories × 2 themes`;
if (failures === 0) {
  console.log(`\n✓ ${checked}: no violations.`);
} else {
  console.log(`\n✗ ${checked}: ${failures} violation${failures === 1 ? "" : "s"}.`);
  process.exit(1);
}
