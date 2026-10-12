# KUI microsite

Published at **https://worxbend.github.io/kui/**. This is the product and documentation
site, separate from the Kafka application in `frontend/`.

## Local preview

Requires Node 22.13 or newer. From the repository root:

```bash
npm ci --prefix website --ignore-scripts
npm run build --prefix website
npm run check --prefix website
PORT=4371 npm run serve --prefix website
```

Open http://localhost:4371/kui/. `SITE_BASE` defaults to `/kui/`; set it consistently
for build, check, and serve when previewing another base path. Rebuild after edits.

With the repository's frontend dependencies and Playwright Chromium installed, run
`npm run test:browser --prefix website` against that preview server. It exercises
navigation, themes, search, gallery dialogs, clipboard controls, responsive layouts,
and representative accessibility checks. Screenshots default to
`/tmp/kui-microsite-screenshots`; set `SCREENSHOT_DIR` to retain them elsewhere.
`SITE_ORIGIN` can point the same checks at the deployed site.
Run `npm run test:a11y --prefix website` for the seven-page accessibility matrix
in both themes. See [verification notes and screenshots](verification/README.md)
for the checks actually performed and their limits.

## Content and structure

- `content/`: task-oriented Markdown guides. Titles, summaries, and navigation live
  in `src/data.mjs`.
- `src/templates.mjs`: shared navigation, footer, dialogs, and page layouts.
- `public/styles.css`: shared design tokens, both themes, components, and responsive rules.
- `public/site.js`: progressive enhancements for search, themes, gallery filters,
  screenshot dialogs, copy feedback, and mobile navigation.
- `scripts/build.mjs`: static HTML generation, Markdown rendering, local search index,
  OpenAPI downloads, sitemap, and Pages-compatible 404.
- `scripts/check.mjs`: all generated internal URLs, assets, heading anchors, unique IDs,
  page landmarks, and guide inventory.

The production, metrics, masking, observability, and gateway-authentication guides
are imported directly from `docs/operations/`, so there is one authoritative copy.
Relative links to those guides are rewritten to site routes; source/configuration
links point to the repository. No backend or registry credentials are needed by the site.

## Assets

All 37 product screenshots originate from the real seeded quickstart in
`docs/frontend/screenshots/current/`. WebP versions retain the original 1440×900
dimensions. After recapturing screenshots, refresh them with:

```bash
python3 website/scripts/optimize-images.py
```

This optional maintenance command requires Pillow; the normal Node build uses the
committed WebP assets. Images below the opening viewport load lazily. Space Grotesk
and Inter are self-hosted, subset variable WOFF2 fonts with their OFL licenses in
`public/fonts/`. No third-party runtime scripts, font requests, or analytics are used.

## Design

“Kafka, in focus” uses large Space Grotesk headings, readable Inter body text,
white and midnight surfaces, electric blue controls, and a branching packet motif
drawn from Kafka partitions. The product is the visual proof: screenshots have
their original colors and can be enlarged. Content alternates between open layouts,
product-led sections, operational steps, and a compact technical reading surface.

Light and dark themes share semantic tokens. A user choice persists when browser
storage is available; otherwise the system color preference applies. Native dialogs
provide focus trapping and Escape handling. Code regions scroll with keyboard focus,
and reduced-motion preferences disable transitions and smooth scrolling.

Design references reviewed: [Linear](https://linear.app/) for product-first presentation
and [Redpanda](https://www.redpanda.com/) for the Kafka audience and technical storytelling.
The site uses its own identity and real KUI content.

## Deploy

`.github/workflows/pages.yml` builds and checks on relevant pull requests, and deploys
on matching pushes to `main` or manual dispatch. Actions are pinned by commit. The
`github-pages` environment and narrowly scoped Pages/OIDC permissions are used only
by the deploy job. GitHub Pages must use **GitHub Actions** as its publishing source.

The workflow uploads `website/dist`; generated output and dependencies are ignored.
Directory-based routes load directly on Pages without an SPA rewrite. The site is
static documentation and cannot connect to a user's Kafka cluster.
