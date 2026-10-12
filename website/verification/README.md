# Microsite verification · 2026-10-13

The delivered site has 28 static routes, 24 documentation guides, a custom 404,
37 optimized screenshots from the existing real quickstart capture, and OpenAPI downloads.
The KUI application code and integrations were not changed.

## Checks performed

- Clean `npm ci --prefix website --ignore-scripts`: completed; npm reported zero vulnerabilities.
- `npm run build --prefix website`: completed.
- `npm run check --prefix website`: checked 29 HTML files including the 404;
  all internal links, assets, anchors, unique IDs, landmarks, and guide inventory passed.
- `npm run test:browser --prefix website`: Chromium visited all 24 guide routes;
  exercised theme switching/persistence, documentation search and its empty state,
  gallery filtering, screenshot enlargement and Escape dismissal, clipboard copying,
  mobile navigation, the mobile guide selector, and reduced motion.
- Viewports: 1440×1000 desktop, 768×1024 tablet, and 390×844 mobile. No horizontal
  page overflow in the checked routes. Horizontally scrolling code blocks remain intentional.
- axe-core WCAG 2 A/AA and 2.1 AA checks: zero violations on the homepage, features,
  gallery, quickstart, consumers, production deployment, and metrics guide in both
  light and dark themes. The mobile quickstart also had zero violations.
  This is automated coverage of representative pages, not a claim of a full WCAG audit.
- Browser console, uncaught exceptions, and failing HTTP responses: none in the final run.
- The new `run-images` Compose example was extracted and validated with
  `docker compose config --quiet` using example registry variables.
- JavaScript syntax checks and `git diff --check`: passed.

## Visual review

No browser surfaces were exposed through the computer-use connector. Screenshots were
captured with the repository's Playwright Chromium installation and inspected with
`view_image`. Capture waits for fonts/images and settled transitions; gallery images
are loaded for full-page screenshots rather than leaving lazy regions blank.

Reviewed the opening headline/CTA hierarchy, screenshot proportions and original
colors, shared alignment and section spacing, light/dark contrast, mobile wrapping,
header navigation, technical prose, tables, and code overflow. The design uses its
own branching packet mark and motif; no raster concept or synthetic product images
were used.

Fixed the preview-server root route, mobile capability-strip overflow, and keyboard
focus for scrolling code blocks. A historical masking-guide anchor was corrected.
The final screenshots match the implemented design direction and have no known
material visual issues in the inspected layouts.

## Screenshots

- [Desktop homepage](home-desktop.png)
- [Dark theme](home-dark.png)
- [Mobile homepage](home-mobile.png)
- [Quickstart guide](quickstart-desktop.png)
- [Mobile filtering guide](filtering-mobile.png)

These are review artifacts, not assets loaded by the microsite.

## Verification limits

The Kafka quickstart stack, JVM builds, registry login/push, OIDC provider, and a
production deployment were not executed for this microsite change. Docker commands
were grounded in the repository's existing deployment scripts and documentation;
registry tagging and pushing were checked against Docker's official references.
Product screenshots reuse the repository's authentic capture set rather than claiming
a new Kafka-stack capture. The Compose syntax check is not an integration test.
