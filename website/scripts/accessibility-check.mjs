import { chromium } from "../../frontend/node_modules/playwright/index.mjs";
import { fileURLToPath } from "node:url";
const axePath = fileURLToPath(
  new URL("../../frontend/node_modules/axe-core/axe.min.js", import.meta.url),
);
const origin = process.env.SITE_ORIGIN || "http://localhost:4371/kui/";
const browser = await chromium.launch();
const page = await browser.newPage({
  viewport: { width: 1440, height: 1000 },
  colorScheme: "light",
});
const failures = [];
for (const theme of ["light", "dark"])
  for (const route of [
    "",
    "features/",
    "screenshots/",
    "docs/quickstart/",
    "docs/consumers/",
    "docs/deployment/",
    "docs/configuration/",
  ]) {
    await page.goto(origin + route);
    await page.evaluate(
      (theme) => (document.documentElement.dataset.theme = theme),
      theme,
    );
    await page.evaluate(() =>
      Promise.all(
        document
          .getAnimations()
          .map((animation) => animation.finished.catch(() => {})),
      ),
    );
    await page.addScriptTag({ path: axePath });
    const result = await page.evaluate(() =>
      axe.run(document, {
        runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21aa"] },
      }),
    );
    if (result.violations.length)
      failures.push({
        theme,
        route,
        violations: result.violations.map((v) => ({
          id: v.id,
          nodes: v.nodes.map((n) => ({
            target: n.target,
            summary: n.failureSummary,
          })),
        })),
      });
    console.log(
      `${theme} /${route || "home"}: ${result.violations.length} violations`,
    );
  }
console.log(JSON.stringify(failures, null, 2));
await browser.close();
if (failures.length) process.exit(1);
