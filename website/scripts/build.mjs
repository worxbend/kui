import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import MarkdownIt from "markdown-it";
import { guides, repo } from "../src/data.mjs";
import {
  base,
  url,
  escape,
  icon,
  layout,
  home,
  featurePage,
  galleryPage,
  docsPage,
} from "../src/templates.mjs";
const root = fileURLToPath(new URL("../", import.meta.url));
const repository = path.resolve(root, ".."),
  dist = path.join(root, "dist");
const imported = {
  deployment: "docs/operations/deployment.md",
  configuration: "docs/operations/configuration.md",
  masking: "docs/operations/masking.md",
  observability: "docs/operations/observability.md",
  "gateway-authentication": "docs/operations/gateway-authentication.md",
};
for (const guide of guides)
  guide.source = imported[guide.slug] || `website/content/${guide.slug}.md`;
const sources = new Map(guides.map((g) => [g.source, g.slug]));
await fs.rm(dist, { recursive: true, force: true });
await fs.mkdir(dist, { recursive: true });
await fs.cp(path.join(root, "public"), dist, { recursive: true });
await fs.mkdir(path.join(dist, "api"));
for (const name of ["openapi.json", "openapi.browser.json"])
  await fs.copyFile(
    path.join(repository, "docs/api", name),
    path.join(dist, "api", name),
  );
const routes = [];
async function writeRoute(route, options) {
  const folder = path.join(dist, route);
  await fs.mkdir(folder, { recursive: true });
  await fs.writeFile(
    path.join(folder, "index.html"),
    layout({ ...options, path: route }),
  );
  routes.push(route);
}
await writeRoute("", {
  title: "KUI",
  description:
    "An open-source Kafka console for cluster health, records, topics, consumers, schemas, Connect, and ksqlDB. Run locally with Docker.",
  content: home(),
});
await writeRoute("features/", {
  title: "Features",
  description:
    "Explore KUI’s record filtering, topic operations, consumer offset plans, replay, Schema Registry, Kafka Connect, ksqlDB, and alerts.",
  content: featurePage(),
  active: "Features",
});
await writeRoute("screenshots/", {
  title: "Screenshots",
  description:
    "37 screenshots of the real KUI Kafka console, captured from the seeded Docker quickstart.",
  content: galleryPage(),
  active: "Screenshots",
});
const search = [];
for (const guide of guides) {
  const source = await fs.readFile(path.join(repository, guide.source), "utf8");
  const toc = [],
    used = new Set();
  const md = new MarkdownIt({ html: false, linkify: true, typographer: false });
  md.renderer.rules.heading_open = (tokens, i) => {
    const level = tokens[i].tag;
    const title = tokens[i + 1].content;
    let id = title
      .toLowerCase()
      .replace(/[^a-z0-9\s-]/g, "")
      .trim()
      .replace(/\s+/g, "-");
    const raw = id;
    let n = 2;
    while (used.has(id)) id = `${raw}-${n++}`;
    used.add(id);
    tokens[i].attrSet("id", id);
    if (level === "h2") toc.push({ id, title });
    return `<${level} id="${id}">`;
  };
  md.renderer.rules.heading_close = (tokens, i) => {
    const open = tokens[i - 2];
    return `${tokens[i].tag === "h2" ? `<a class="heading-anchor" href="#${escape(open.attrGet("id"))}" aria-label="Link to ${escape(tokens[i - 1].content)}">#</a>` : ""}</${tokens[i].tag}>\n`;
  };
  md.renderer.rules.fence = (tokens, i) =>
    `<pre><span class="code-label">${escape(tokens[i].info || "Code")}</span><button class="copy-code" aria-label="Copy code">${icon("copy")}Copy</button><code tabindex="0">${escape(tokens[i].content)}</code></pre>`;
  const originalLink =
    md.renderer.rules.link_open ||
    ((tokens, i, opts, env, self) => self.renderToken(tokens, i, opts));
  md.renderer.rules.link_open = (tokens, i, opts, env, self) => {
    const token = tokens[i],
      href = token.attrGet("href");
    if (href?.startsWith("/")) token.attrSet("href", url(href));
    else if (
      imported[guide.slug] &&
      href &&
      !/^(https?:|#|mailto:)/.test(href)
    ) {
      const [target, hash] = href.split("#");
      const file = path.posix.normalize(
        path.posix.join(path.posix.dirname(guide.source), target),
      );
      token.attrSet(
        "href",
        sources.has(file)
          ? `${url(`docs/${sources.get(file)}/`)}${hash ? `#${hash}` : ""}`
          : `${repo}/blob/main/${file}${hash ? `#${hash}` : ""}`,
      );
    }
    return originalLink(tokens, i, opts, env, self);
  };
  const originalImage = md.renderer.rules.image;
  md.renderer.rules.image = (tokens, i, opts, env, self) => {
    const token = tokens[i],
      src = token.attrGet("src");
    if (src?.startsWith("/")) token.attrSet("src", url(src));
    token.attrSet("loading", "lazy");
    token.attrSet("decoding", "async");
    token.attrSet("width", "1440");
    token.attrSet("height", "900");
    return originalImage(tokens, i, opts, env, self);
  };
  const tokens = md.parse(source, {});
  if (tokens[0]?.type === "heading_open" && tokens[0].tag === "h1")
    tokens.splice(0, 3);
  let body = md.renderer.render(tokens, md.options, {});
  body = body
    .replace(/<table>/g, '<div class="table-wrap"><table>')
    .replace(/<\/table>/g, "</table></div>");
  await writeRoute(`docs/${guide.slug}/`, {
    title: guide.title,
    description: guide.description,
    content: docsPage(guide, body, toc),
    active: "Documentation",
    doc: true,
  });
  search.push({
    ...guide,
    url: url(`docs/${guide.slug}/`),
    text: source.replace(/[#*`|\[\]]/g, ""),
  });
}
await writeRoute("docs/", {
  title: "Documentation",
  description: "KUI documentation and practical Kafka guides.",
  content: `<main id="main" class="wrap page-intro"><h1>Find your next step.</h1><p>Product workflows, Docker setup, and deployment guides.</p><div class="ecosystem-list">${guides.map((g) => `<a href="${url(`docs/${g.slug}/`)}"><h3>${g.title}</h3><p>${g.description}</p>${icon("arrow")}</a>`).join("")}</div></main>`,
  active: "Documentation",
});
await fs.writeFile(path.join(dist, "search.json"), JSON.stringify(search));
await fs.writeFile(
  path.join(dist, "404.html"),
  layout({
    title: "Page not found",
    description: "Return to KUI documentation.",
    content: `<main id="main" class="wrap page-intro"><h1>Lost in the stream?</h1><p>This page could not be found.</p><div class="actions"><a class="button" href="${url("")}">Back to KUI ${icon("arrow")}</a><a class="button secondary" href="${url("docs/introduction/")}">Open documentation</a></div></main>`,
  }),
);
await fs.writeFile(path.join(dist, ".nojekyll"), "");
await fs.writeFile(
  path.join(dist, "robots.txt"),
  `User-agent: *\nAllow: /\nSitemap: https://worxbend.github.io${url("sitemap.xml")}\n`,
);
await fs.writeFile(
  path.join(dist, "sitemap.xml"),
  `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${routes.map((route) => `<url><loc>https://worxbend.github.io${url(route)}</loc></url>`).join("")}</urlset>`,
);
console.log(
  `Built ${routes.length} static pages, ${guides.length} searchable guides, and 37 screenshots at ${base}`,
);
