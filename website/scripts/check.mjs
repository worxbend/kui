import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { base } from "../src/templates.mjs";
const dist = fileURLToPath(new URL("../dist/", import.meta.url));
async function files(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  return (
    await Promise.all(
      entries.map((e) =>
        e.isDirectory()
          ? files(path.join(dir, e.name))
          : [path.join(dir, e.name)],
      ),
    )
  ).flat();
}
const all = await files(dist),
  html = all.filter((f) => f.endsWith(".html")),
  errors = [];
for (const file of html) {
  const source = await fs.readFile(file, "utf8");
  const relative = path.relative(dist, file);
  const origin = `https://check.invalid${base}${relative.replace(/index\.html$/, "")}`;
  if (
    !source.includes('<main id="main"') ||
    !source.includes('<html lang="en"')
  )
    errors.push(`${relative}: missing landmark/language`);
  if ((source.match(/<h1[ >]/g) || []).length !== 1)
    errors.push(`${relative}: expected one h1`);
  const ids = [...source.matchAll(/\bid="([^"]+)"/g)].map((m) => m[1]);
  if (ids.length !== new Set(ids).size)
    errors.push(`${relative}: duplicate IDs`);
  for (const match of source.matchAll(/(?:href|src)="([^"]+)"/g)) {
    const raw = match[1];
    if (/^(https?:|mailto:|data:)/.test(raw)) continue;
    const resolved = new URL(raw, origin);
    if (!resolved.pathname.startsWith(base)) {
      errors.push(`${relative}: link escapes Pages base: ${raw}`);
      continue;
    }
    let target = path.join(
      dist,
      decodeURIComponent(resolved.pathname.slice(base.length)),
    );
    if (target.endsWith("/") || !path.extname(target))
      target = path.join(target, "index.html");
    try {
      await fs.access(target);
      if (resolved.hash && target.endsWith(".html")) {
        const targetHtml = await fs.readFile(target, "utf8");
        if (
          !targetHtml.includes(
            `id="${decodeURIComponent(resolved.hash.slice(1))}"`,
          )
        )
          errors.push(`${relative}: missing anchor ${raw}`);
      }
    } catch {
      errors.push(`${relative}: missing target ${raw}`);
    }
  }
}
const search = JSON.parse(
  await fs.readFile(path.join(dist, "search.json"), "utf8"),
);
if (search.length !== 24)
  errors.push(`Expected 24 guides, found ${search.length}`);
if (errors.length) {
  console.error(errors.join("\n"));
  process.exit(1);
}
console.log(
  `Checked ${html.length} HTML files: internal links, assets, anchors, unique IDs, landmarks, and 24 indexed guides.`,
);
