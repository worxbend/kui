# Storybook: the design system and every screen state, as a static site.
#
# ## Why this is worth an image of its own
#
# Storybook is where this frontend is actually reviewed. Every state that matters — a cluster that
# is not answering, a figure nobody could measure, a broker whose disk read failed, a principal who
# may look but not change — needs a broken cluster to reach in the running product and one click to
# reach here. Handing a designer or a reviewer a URL rather than "install Node 22.12, install pnpm
# 11.25, run three commands" is the difference between that review happening and not.
#
# It needs no backend. Every story renders from fixtures, by design: a story that fetched would only
# be viewable when the thing it fetched from was healthy, which is the opposite of the point.
#
# ## Two stages, and why the second is not Node
#
# The build stage needs the whole pnpm toolchain. The result is a directory of static files, so the
# thing that serves them needs no JavaScript runtime at all — a Node process serving static assets
# would be about 150 MB of attack surface for something nginx does in 20.
FROM node:22.22.0-bookworm-slim AS build

# pnpm, pinned, installed directly.
#
# Corepack would be the tidier answer — it reads the exact version out of `frontend/package.json`'s
# `packageManager` field, so there would be one number rather than two. It does not work: the
# corepack bundled with Node 22.12 carries signing keys that no longer match npm's, and every
# invocation dies with `Cannot find matching keyid` before it fetches anything. Tried, and this is
# what it fails with.
#
# So the version is written here as well, and it must match `packageManager` in
# `frontend/package.json` and `pnpm` in `.tool-versions`. `pnpm install` itself checks: it refuses
# to run when the running version disagrees with the manifest, which is what stops the two drifting
# silently.
ARG PNPM_VERSION=11.25.0
RUN npm install --global --no-fund --no-audit "pnpm@${PNPM_VERSION}"

WORKDIR /build

# The manifests first, and only the manifests. Docker caches a layer by the files that went into it,
# so an edit to a component invalidates the layer below this one and not the install itself — which
# is the difference between a 4-second rebuild and a 90-second one.
#
# EVERY workspace manifest, and the last three lines are a repair rather than a preference. This list
# stopped at `feature-schemas` for three packages' worth of history: `pnpm-lock.yaml` declares eleven
# importers and this file copied eight, so `--frozen-lockfile` below had nothing to be frozen against
# for `feature-alerts`, `feature-connect` and `feature-ksql`, and each of those three ships a
# `.stories.` file that is the entire reason this image exists. Nothing built the image, so nothing
# said so. `ShippedImageManifestSuite` now derives the roster with `git ls-files` and compares it
# against every Dockerfile under `deployment/` in both directions, so a twelfth package that is not
# copied here fails a test instead of an image build nobody runs.
COPY frontend/package.json frontend/pnpm-lock.yaml frontend/pnpm-workspace.yaml ./
COPY frontend/packages/api/package.json ./packages/api/
COPY frontend/packages/kernel/package.json ./packages/kernel/
COPY frontend/packages/shell/package.json ./packages/shell/
COPY frontend/packages/feature-clusters/package.json ./packages/feature-clusters/
COPY frontend/packages/feature-topics/package.json ./packages/feature-topics/
COPY frontend/packages/feature-messages/package.json ./packages/feature-messages/
COPY frontend/packages/feature-consumers/package.json ./packages/feature-consumers/
COPY frontend/packages/feature-schemas/package.json ./packages/feature-schemas/
COPY frontend/packages/feature-alerts/package.json ./packages/feature-alerts/
COPY frontend/packages/feature-connect/package.json ./packages/feature-connect/
COPY frontend/packages/feature-ksql/package.json ./packages/feature-ksql/

# `--frozen-lockfile` so a lockfile that has drifted from the manifests fails the build here, rather
# than silently resolving to different versions than a developer has. That is the whole reason a
# lockfile is committed.
RUN --mount=type=cache,id=pnpm-store,target=/pnpm-store \
    pnpm config set store-dir /pnpm-store && \
    pnpm install --frozen-lockfile

# Now the source. Everything above this line is cached across source edits.
COPY frontend/ ./

# `--quiet` because Storybook's build log is 200 lines of progress bars that render as noise in a
# non-interactive build, and a failure still prints.
RUN pnpm exec storybook build --output-dir /storybook --quiet

# ------------------------------------------------------------------------------------------------

# The *unprivileged* nginx image, not the ordinary one.
#
# Stock `nginx` starts as root, chowns its cache directories and then drops to `nginx`. That is
# incompatible with the `cap_drop: ALL` this container runs under: the chown fails with
# "Operation not permitted" and nginx exits before it binds, so the container restart-loops with a
# message that looks nothing like a permissions problem. Tried, and that is what it does.
#
# `nginx-unprivileged` runs as uid 101 throughout and keeps its temporary paths under `/tmp`, so it
# needs no capability at all. It also cannot bind below port 1024, which is why this listens on
# 8081.
FROM nginxinc/nginx-unprivileged:1.27-alpine

# Storybook is a single-page application: it routes on the fragment and the path, so a deep link to
# one story must be answered with `index.html` rather than a 404. Without this, every link anybody
# shares is broken for the person who receives it — which is the one thing this image exists for.
RUN printf '%s\n' \
    'server {' \
    '  listen 8081;' \
    '  root /usr/share/nginx/html;' \
    '  location / { try_files $uri $uri/ /index.html; }' \
    '  # The hashed assets are immutable; index.html must never be, or a redeploy serves the old one.' \
    '  location ~* \.(js|css|woff2?|png|svg)$ { expires 1y; add_header Cache-Control "public, immutable"; }' \
    '  location = /index.html { add_header Cache-Control "no-cache"; }' \
    '}' > /etc/nginx/conf.d/default.conf

COPY --from=build /storybook /usr/share/nginx/html

EXPOSE 8081/tcp
