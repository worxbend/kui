import { chromium } from "../../frontend/node_modules/playwright/index.mjs";
import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";
const screenshots =
  process.env.SCREENSHOT_DIR || "/tmp/kui-microsite-screenshots";
await fs.mkdir(screenshots, { recursive: true });
const axePath = fileURLToPath(
  new URL("../../frontend/node_modules/axe-core/axe.min.js", import.meta.url),
);
const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 1440, height: 1000 },
  colorScheme: "light",
  permissions: ["clipboard-read", "clipboard-write"],
});
const page = await context.newPage();
const errors = [];
page.on("pageerror", (error) => errors.push(error.message));
page.on("console", (msg) => {
  if (msg.type() === "error") errors.push(msg.text());
});
page.on("response", (res) => {
  if (res.status() >= 400) errors.push(`${res.status()} ${res.url()}`);
});
const origin = process.env.SITE_ORIGIN || "http://localhost:4371/kui/";
const capture = async (name) => {
  await page.evaluate(async () => {
    await document.fonts.ready;
    for (const img of document.images) img.loading = "eager";
    await Promise.all(
      [...document.images].map((img) => img.decode().catch(() => {})),
    );
  });
  await page.evaluate(() =>
    Promise.all(
      document.getAnimations().map((a) => a.finished.catch(() => {})),
    ),
  );
  await page.screenshot({ path: `${screenshots}/${name}.png`, fullPage: true });
};
await page.goto(origin);
await capture("home-desktop");
await page.getByRole("button", { name: "Switch to dark theme" }).click();
if ((await page.locator("html").getAttribute("data-theme")) !== "dark")
  throw Error("Theme did not change");
await capture("home-dark");
await page.reload();
if ((await page.locator("html").getAttribute("data-theme")) !== "dark")
  throw Error("Theme did not persist");
await page.getByRole("button", { name: "Switch to light theme" }).click();
await page
  .getByRole("button", { name: "Search documentation", exact: true })
  .click();
await page
  .getByRole("searchbox", { name: "Search guides" })
  .fill("offset reset");
await page.locator(".search-result").first().waitFor();
await page
  .locator("#search-results")
  .getByRole("link")
  .filter({ hasText: "Consumer lag & offset plans" })
  .click();
await page.waitForURL("**/docs/consumers/");
await capture("consumers-desktop");
await page.goto(origin + "docs/quickstart/");
await page.locator(".copy-code").first().click();
if (
  !(await page.evaluate(() => navigator.clipboard.readText())).includes(
    "git clone",
  )
)
  throw Error("Wrong clipboard content");
await capture("quickstart-desktop");
await page.goto(origin + "screenshots/");
await page.getByRole("button", { name: "Records", exact: true }).click();
if ((await page.locator("[data-gallery-category]:visible").count()) !== 10)
  throw Error("Gallery filter count");
await page.getByRole("button", { name: "Enlarge Decoded JSON filter" }).click();
if (!(await page.locator("#image-dialog").isVisible()))
  throw Error("Image dialog did not open");
await page.keyboard.press("Escape");
if (await page.locator("#image-dialog").isVisible())
  throw Error("Image dialog did not close");
await page.getByRole("button", { name: "All screens", exact: true }).click();
await capture("gallery-desktop");
await page.goto(origin + "features/");
await capture("features-desktop");
const index = await (await fetch(origin + "search.json")).json();
for (const item of index) {
  await page.goto(new URL(item.url, origin).href);
  if ((await page.locator("h1").count()) !== 1)
    throw Error(`Heading at ${item.url}`);
  if (
    await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)
  )
    throw Error(`Overflow: ${item.url}`);
}
await page.addScriptTag({ path: axePath });
const a11y = await page.evaluate(() =>
  axe.run(document, {
    runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21aa"] },
  }),
);
const violations = a11y.violations.map((v) => ({
  id: v.id,
  help: v.help,
  nodes: v.nodes.map((n) => n.target),
}));
await page.setViewportSize({ width: 768, height: 1024 });
await page.goto(origin);
await capture("home-tablet");
if (
  await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)
)
  throw Error("Tablet overflow");
await page.goto(origin + "docs/consumers/");
await capture("consumers-tablet");
await page.setViewportSize({ width: 390, height: 844 });
await page.goto(origin);
await capture("home-mobile");
await page.getByRole("button", { name: "Open navigation" }).click();
await page
  .locator("#primary-nav")
  .getByRole("link", { name: "Documentation", exact: true })
  .click();
await page.waitForURL("**/docs/introduction/");
await page.getByLabel("Choose a guide").selectOption("/kui/docs/filtering/");
await page.waitForURL("**/docs/filtering/");
await capture("filtering-mobile");
for (const route of [
  "",
  "features/",
  "screenshots/",
  "docs/run-images/",
  "docs/deployment/",
  "docs/masking/",
]) {
  await page.goto(origin + route);
  if (
    await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)
  )
    throw Error(`Mobile overflow ${route}`);
}
await page.goto(origin + "docs/quickstart/");
await page.addScriptTag({ path: axePath });
const mobileA11y = await page.evaluate(() =>
  axe.run(document, {
    runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21aa"] },
  }),
);
await page
  .getByRole("button", { name: "Search documentation", exact: true })
  .click();
await page.getByRole("searchbox").fill("zzzznonexistent");
await page.waitForFunction(() =>
  document.querySelector("#search-status").textContent.startsWith("No guides"),
);
await page.keyboard.press("Escape");
console.log(
  JSON.stringify(
    {
      routesChecked: index.length,
      viewports: ["1440x1000", "768x1024", "390x844"],
      errors,
      violations,
      mobileViolations: mobileA11y.violations.map((v) => ({
        id: v.id,
        nodes: v.nodes.map((n) => n.target),
      })),
      screenshots,
    },
    null,
    2,
  ),
);
await fs.writeFile(
  `${screenshots}/results.json`,
  JSON.stringify(
    { errors, violations, mobileViolations: mobileA11y.violations },
    null,
    2,
  ),
);
await page.emulateMedia({ reducedMotion: "reduce" });
if (
  (await page.evaluate(
    () => getComputedStyle(document.documentElement).scrollBehavior,
  )) !== "auto"
)
  throw Error("Reduced motion preference ignored");
await browser.close();
if (errors.length || violations.length || mobileA11y.violations.length)
  process.exit(1);
