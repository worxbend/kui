import { repo, sections, guides, features, gallery } from "./data.mjs";
export const base = (process.env.SITE_BASE || "/kui/").replace(/\/?$/, "/");
export const url = (path) => `${base}${path.replace(/^\//, "")}`;
export const escape = (s) =>
  String(s).replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
export const icon = (name, cls = "") => {
  const paths = {
    arrow: '<path d="M5 12h14m-6-6 6 6-6 6"/>',
    external: '<path d="M14 4h6v6m0-6L10 14"/><path d="M10 4H4v16h16v-6"/>',
    search: '<circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 5 5"/>',
    sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5"/>',
    moon: '<path d="M20 15A9 9 0 0 1 9 4a9 9 0 1 0 11 11Z"/>',
    menu: '<path d="M4 7h16M4 12h16M4 17h16"/>',
    close: '<path d="m6 6 12 12M6 18 18 6"/>',
    copy: '<rect x="8" y="8" width="12" height="12" rx="2"/><path d="M16 8V4H4v12h4"/>',
    check: '<path d="m5 12 4 4L19 6"/>',
    github:
      '<path d="M9 19c-4 1-4-2-6-2m12 5v-3.9c0-1.1-.4-1.8-.8-2.2 2.7-.3 5.5-1.3 5.5-6a4.7 4.7 0 0 0-1.3-3.3 4.3 4.3 0 0 0-.1-3.3s-1-.3-3.4 1.3a11.7 11.7 0 0 0-6.2 0C6.3 2 5.3 2.3 5.3 2.3a4.3 4.3 0 0 0-.1 3.3 4.7 4.7 0 0 0-1.3 3.3c0 4.7 2.8 5.7 5.5 6-.4.4-.8 1.1-.8 2.2V22"/>',
  };
  return `<svg class="icon ${cls}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[name] || paths.arrow}</svg>`;
};
export const mark = `<svg viewBox="0 0 32 32" fill="none" aria-hidden="true"><path d="M7 16h8m0 0 8-9m-8 9 8 9" stroke="currentColor" stroke-width="3"/><rect x="2" y="11" width="9" height="10" rx="3" fill="currentColor"/><rect x="21" y="2" width="9" height="10" rx="3" fill="currentColor"/><rect x="21" y="20" width="9" height="10" rx="3" fill="currentColor"/></svg>`;
export const button = (label, href, secondary = false) =>
  `<a class="button ${secondary ? "secondary" : ""}" href="${href}">${label}${icon("arrow")}</a>`;
export const screenshot = (image, alt, eager = false) =>
  `<img src="${url(`screenshots/${image}.webp`)}" alt="${escape(alt)}" width="1440" height="900" ${eager ? 'fetchpriority="high"' : 'loading="lazy"'} decoding="async">`;
export const screen = (image, alt, eager = false) =>
  `<figure class="product-screen"><button class="zoom-image" data-image="${url(`screenshots/${image}.webp`)}" data-title="${escape(alt)}" aria-label="Enlarge ${escape(alt)}">${screenshot(image, alt, eager)}</button></figure>`;
export const motif = `<svg class="stream-motif" viewBox="0 0 500 360" fill="none" aria-hidden="true"><g stroke="currentColor" stroke-width="2"><path d="M0 180h160c60 0 65-115 130-115h210"/><path d="M0 180h500"/><path d="M0 180h160c60 0 65 115 130 115h210"/></g><g fill="currentColor"><rect x="70" y="171" width="35" height="18" rx="5"/><rect x="324" y="56" width="35" height="18" rx="5"/><rect x="260" y="171" width="35" height="18" rx="5"/><rect x="388" y="286" width="35" height="18" rx="5"/></g></svg>`;
export function layout({
  title,
  description,
  content,
  active = "",
  path = "",
  doc = false,
}) {
  const nav = [
    ["Features", "features/"],
    ["Screenshots", "screenshots/"],
    ["Documentation", "docs/introduction/"],
  ];
  return `<!doctype html><html lang="en" data-theme="light"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escape(title)}${title === "KUI" ? " — Kafka, in focus." : " · KUI"}</title><meta name="description" content="${escape(description)}"><meta name="theme-color" content="#f7f9fd"><meta property="og:title" content="${escape(title)} · KUI"><meta property="og:description" content="${escape(description)}"><meta property="og:type" content="website"><meta property="og:image" content="https://worxbend.github.io${url("screenshots/05-cluster-overview.webp")}"><link rel="canonical" href="https://worxbend.github.io${url(path)}"><link rel="icon" href="${url("favicon.svg")}" type="image/svg+xml"><link rel="preload" href="${url("fonts/space.woff2")}" as="font" type="font/woff2" crossorigin><link rel="stylesheet" href="${url("styles.css")}"><script>try{const t=localStorage.getItem('kui-site-theme');if(t==='dark'||t==='light')document.documentElement.dataset.theme=t;else if(matchMedia('(prefers-color-scheme: dark)').matches)document.documentElement.dataset.theme='dark'}catch{}</script><script src="${url("site.js")}" defer></script></head><body class="${doc ? "documentation" : ""}" data-base="${base}"><a class="skip-link" href="#main">Skip to content</a><header class="site-header"><div class="header-inner"><a class="brand" href="${url("")}" aria-label="KUI home">${mark}<span>KUI</span></a><nav id="primary-nav" aria-label="Main navigation">${nav.map(([label, p]) => `<a href="${url(p)}" ${active === label ? 'aria-current="page"' : ""}>${label}</a>`).join("")}</nav><div class="header-actions"><button class="icon-button search-trigger" aria-label="Search documentation">${icon("search")}<kbd>/</kbd></button><button class="icon-button theme-toggle" aria-label="Switch to dark theme">${icon("sun", "sun-icon")}${icon("moon", "moon-icon")}</button><a class="github-link" href="${repo}" aria-label="KUI on GitHub">${icon("github")}<span>GitHub</span></a><button class="icon-button menu-toggle" aria-label="Open navigation" aria-controls="primary-nav" aria-expanded="false">${icon("menu")}</button></div></div></header>${content}<footer class="site-footer"><div class="footer-top"><a class="brand" href="${url("")}">${mark}<span>KUI</span></a><p>A clearer view of your Kafka.</p><div><a href="${url("docs/quickstart/")}">Get started</a><a href="${repo}">Source code</a><a href="${repo}/issues">Report an issue</a></div></div><div class="footer-bottom"><span>Open source. Apache 2.0.</span><span>Built with Scala, Cats Effect, FS2 & SolidJS.</span><a href="${repo}/blob/main/LICENSE">License</a></div></footer><dialog id="search-dialog" class="search-dialog" aria-labelledby="search-title"><div class="dialog-heading"><h2 id="search-title">Search documentation</h2><button class="icon-button" data-close aria-label="Close search">${icon("close")}</button></div><label class="sr-only" for="search-input">Search guides</label><input id="search-input" type="search" placeholder="Try ‘offset reset’ or ‘Docker’" autocomplete="off"><p id="search-status" role="status">Search features, guides, and deployment.</p><div id="search-results"></div></dialog><dialog id="image-dialog" class="image-dialog" aria-labelledby="image-title"><div class="dialog-heading"><h2 id="image-title"></h2><button class="icon-button" data-close aria-label="Close screenshot">${icon("close")}</button></div><img id="enlarged-image" alt="" width="1440" height="900"><a id="image-original">Open full-size image ${icon("external")}</a></dialog><div class="toast" role="status" aria-live="polite"></div></body></html>`;
}
export function home() {
  return `<main id="main"><section class="hero wrap"><div class="hero-copy"><h1>Kafka,<br>in focus.</h1><div class="hero-description"><p>Your clusters. Your records. Your next move.<br>KUI brings Kafka operations into one clear view.</p><div class="actions">${button("Run KUI locally", url("docs/quickstart/"))}${button("Explore the features", url("features/"), true)}</div><p class="hero-meta">Open source <span></span> Self-hosted <span></span> Apache 2.0</p></div></div><div class="hero-product"><div class="screen-caption"><span><i class="status-dot"></i> Your Kafka control room</span><span>Captured from the real quickstart</span></div>${screen("05-cluster-overview", "KUI cluster overview with broker health, throughput, storage, and consumer lag", true)}${motif}</div></section><div class="capability-strip"><div class="wrap"><span>Built for the work behind the stream.</span><p>Inspect<span>/</span>Filter<span>/</span>Replay<span>/</span>Operate</p></div></div><section class="section wrap"><div class="section-heading"><h2>Less switching.<br>More understanding.</h2><p>From the health of a cluster to the contents of a single event. Keep the whole picture in reach.</p></div><div class="feature-index">${features
    .slice(0, 3)
    .map(
      (f) =>
        `<a href="${url(`features/#${f.id}`)}"><span class="feature-symbol">${f.id === "clusters" ? mark : f.id === "messages" ? icon("search") : icon("check")}</span><h3>${f.short}</h3><p>${f.description}</p><span class="text-link">Explore ${icon("arrow")}</span></a>`,
    )
    .join(
      "",
    )}</div></section><section class="record-section"><div class="wrap split-feature"><div class="feature-copy"><h2>Find the signal<br>inside the stream.</h2><p>Read the payload, not just the bytes. Filter decoded JSON, Avro, JSON Schema, and Protobuf fields. Seek to a timestamp. Follow the record that explains the problem.</p><a class="text-link" href="${url("docs/filtering/")}">Get to know record filtering ${icon("arrow")}</a><div class="mini-code"><span>Advanced filter · CEL</span><code>record.value.status == "DECLINED"</code></div></div>${screen("26-filter-json", "KUI filtering records by a decoded JSON field")}</div></section><section class="section wrap"><div class="section-heading"><h2>One workspace.<br>Your Kafka ecosystem.</h2><p>Topics, consumers, schemas, connectors, and queries. The tools you need, with shared context and consistent guardrails.</p></div><div class="ecosystem-list">${features
    .slice(3)
    .map(
      (f) =>
        `<a href="${url(`features/#${f.id}`)}"><h3>${f.short}</h3><p>${f.details[0]}</p>${icon("arrow")}</a>`,
    )
    .join(
      "",
    )}</div><div class="gallery-callout"><span>See the product, screen by screen.</span><a class="text-link" href="${url("screenshots/")}">Browse all 37 screenshots ${icon("arrow")}</a></div></section><section class="safety-section"><div class="wrap safety-layout"><div><h2>A preview before<br>the point of no return.</h2><p>Risky changes deserve a clear decision. KUI uses plan → token → confirm workflows for destructive operations, alongside read-only mode, authentication, and role-based permissions.</p><a class="text-link" href="${url("docs/consumers/")}">Understand consumer offset plans ${icon("arrow")}</a></div><div class="plan-steps"><div><span>1</span><h3>Choose the change</h3><p>Select the topic and target offsets.</p></div><div><span>2</span><h3>Review the plan</h3><p>See current and proposed positions per partition.</p></div><div><span>3</span><h3>Apply with intent</h3><p>Confirm the exact plan, then inspect the receipt.</p></div></div></div></section><section class="section wrap start-section"><div><h2>Your Kafka.<br>Your infrastructure.</h2><p>Try the complete Docker playground, or build and publish images for your own environment.</p><div class="actions">${button("Start with Docker", url("docs/quickstart/"))}${button("Deployment guide", url("docs/deployment/"), true)}</div></div><div class="terminal"><div class="terminal-heading"><span><i></i><i></i><i></i></span><span>Terminal</span></div><pre><code>git clone ${repo}.git
cd kui
deployment/quickstart/quickstart.sh</code></pre><p>Then open <strong>localhost:8090/ui/</strong></p></div></section><div class="release-note wrap"><p>KUI is pre-1.0. Images are built from source. Authentication is disabled in the local demo.</p><a href="${url("docs/security/")}">Prepare for production ${icon("arrow")}</a></div></main>`;
}
export function featurePage() {
  return `<main id="main"><section class="page-intro wrap"><h1>Meet your<br>Kafka workspace.</h1><p>From the first event to the next operational decision.<br>Explore what KUI puts within reach.</p></section><nav class="feature-tabs wrap" aria-label="Feature sections">${features.map((f) => `<a href="#${f.id}">${f.short}</a>`).join("")}</nav>${features.map((f, i) => `<section id="${f.id}" class="feature-band ${i % 2 ? "alternate" : ""}"><div class="wrap split-feature ${i % 2 ? "reversed" : ""}"><div class="feature-copy"><span class="section-label">${f.short}</span><h2>${f.title}</h2><p>${f.description}</p><ul class="detail-list">${f.details.map((d) => `<li>${icon("check")}${d}</li>`).join("")}</ul><a class="text-link" href="${url(`docs/${f.guide}/`)}">Read the guide ${icon("arrow")}</a></div>${screen(f.image, `KUI ${f.short}`)}</div></section>`).join("")}<section class="wrap compact-cta"><h2>See it on your own machine.</h2>${button("Run KUI locally", url("docs/quickstart/"))}</section><p class="scope-note wrap">KUI is pre-1.0. ACL and client-quota management are outside the current scope. Available actions depend on permissions, read-only settings, and configured integrations.</p></main>`;
}
export function galleryPage() {
  const cats = [...new Set(gallery.map((g) => g.category))];
  return `<main id="main"><section class="page-intro wrap"><h1>The real product.<br>Every angle.</h1><p>37 screens from the seeded Docker quickstart.<br>Actual workflows, decoded records, and honest empty states.</p></section><section class="gallery-section wrap" aria-label="Screenshot gallery"><div class="gallery-controls"><div class="filter-buttons" role="group" aria-label="Filter screenshots">${["All screens", ...cats].map((c, i) => `<button data-category="${i ? c : "all"}" aria-pressed="${!i}">${c}</button>`).join("")}</div><span id="gallery-count" role="status">37 screens</span></div><div class="gallery-grid">${gallery.map((g) => `<figure data-gallery-category="${g.category}"><button class="zoom-image" data-image="${url(`screenshots/${g.image}.webp`)}" data-title="${escape(g.title)}" aria-label="Enlarge ${escape(g.title)}">${screenshot(g.image, g.title)}</button><figcaption><h2>${g.title}</h2><span>${g.category}</span></figcaption></figure>`).join("")}</div></section><section class="wrap compact-cta"><h2>Make it your workspace.</h2>${button("Run the quickstart", url("docs/quickstart/"))}</section></main>`;
}
export function docsPage(guide, body, toc) {
  const index = guides.indexOf(guide),
    previous = guides[index - 1],
    next = guides[index + 1];
  return `<main id="main" class="docs-layout wrap"><aside class="docs-sidebar"><a class="docs-home" href="${url("docs/introduction/")}">KUI documentation</a><button class="docs-search search-trigger">${icon("search")} Search guides <kbd>/</kbd></button><nav aria-label="Documentation">${sections.map((s) => `<div class="sidebar-group"><h2>${s.name}</h2>${s.pages.map(([slug, title]) => `<a href="${url(`docs/${slug}/`)}" ${slug === guide.slug ? 'aria-current="page"' : ""}>${title}</a>`).join("")}</div>`).join("")}</nav></aside><div class="docs-main"><div class="mobile-doc-nav"><label for="guide-select">Choose a guide</label><select id="guide-select">${sections.map((s) => `<optgroup label="${s.name}">${s.pages.map(([slug, title]) => `<option value="${url(`docs/${slug}/`)}" ${slug === guide.slug ? "selected" : ""}>${title}</option>`).join("")}</optgroup>`).join("")}</select></div><div class="doc-breadcrumb"><a href="${url("docs/introduction/")}">Documentation</a><span>/</span><span>${guide.group}</span></div><article class="prose"><h1>${guide.title}</h1><p class="doc-lead">${guide.description}</p>${body}</article><div class="doc-pagination">${previous ? `<a href="${url(`docs/${previous.slug}/`)}"><span>Previous guide</span><strong>${previous.title}</strong></a>` : "<div></div>"}${next ? `<a href="${url(`docs/${next.slug}/`)}"><span>Next guide</span><strong>${next.title}</strong>${icon("arrow")}</a>` : ""}</div><a class="edit-link" href="${repo}/blob/main/${guide.source}">View this guide on GitHub ${icon("external")}</a></div><aside class="toc"><nav aria-label="On this page"><h2>On this page</h2>${toc.map((t) => `<a href="#${t.id}">${escape(t.title)}</a>`).join("")}</nav><a class="toc-start" href="${url("docs/quickstart/")}">New to KUI?<br><strong>Start locally ${icon("arrow")}</strong></a></aside></main>`;
}
