# The KUI interface, as its own image.
#
# ## Why the frontend is a separate image at all
#
# It talks to the backend over the same HTTP API every other client uses, so there is nothing it
# needs from being inside the gateway's jar — and two things it loses. The backend then builds and
# tests with nothing but a JDK, which is a real saving for anybody working on a Kafka adapter who
# should not have to install Node to run a suite. And the interface can be rebuilt, rolled forward
# or rolled back on its own: a stylesheet fix does not reassemble a Scala jar or restart a process
# holding Kafka connections.
#
# ## Why it proxies the API rather than the browser calling the gateway directly
#
# Same origin. The session is a cookie and every mutation carries a CSRF header (ADR-019); pointing
# the browser at a second origin means CORS, `SameSite` decisions and a preflight on every mutation,
# all to avoid one `proxy_pass`. So the browser asks this nginx for `/api/…` and this nginx asks the
# gateway. The cookie and the CSRF token then behave exactly as they do when one process serves both.
#
# ## Configuration
#
# Everything that varies between deployments is read at *container start*, not baked in at build
# time, so one image runs in every environment:
#
#   KUI_GATEWAY_URL   where the API is            (default http://kui:8080)
#   KUI_BASE_PATH     mount point behind a proxy  (default empty, i.e. served at /)
#   KUI_BUILD_VERSION shown on the settings page  (default "unknown")
#
# ------------------------------------------------------------------------------------------------
# Stage 1: build the interface.
# ------------------------------------------------------------------------------------------------
FROM node:22.22.0-bookworm-slim AS build

# Not corepack: the corepack bundled with this Node carries signing keys npm no longer matches, and
# it fails with "Cannot find matching keyid" — a message that sends people looking at their registry.
ARG PNPM_VERSION=11.25.0
RUN npm install --global --no-fund --no-audit "pnpm@${PNPM_VERSION}"

WORKDIR /build

# The manifests first, so a source change does not re-run the install. Each package's own manifest
# has to be here for `pnpm install --frozen-lockfile` to resolve the workspace.
#
# THIS LIST IS A HAND-WRITTEN ROSTER AND IT HAD DRIFTED BY THREE. `frontend/packages/` holds eleven
# packages; this list named eight. `feature-alerts`, `feature-connect` and `feature-ksql` — added in
# waves 7 and 8 by packets that did not own this file — were missing, and nothing anywhere noticed:
# the image still built and still carried all eight feature chunks. Which later step recovered the
# omission I did not establish and am not going to guess at — `COPY frontend/ ./` below does bring
# the three manifests in, and that is where to start looking. What is measured is that the list was
# wrong for two waves with every gate green, which is this repository's most-repeated defect.
#
# It cannot be a glob: `COPY frontend/packages/*/package.json` flattens the matches into one
# directory and there is no Docker form that preserves the per-package path. It is therefore a
# roster, and the sentence that stood here said a roster "has to be checked by a reader", with the
# two commands a reader was meant to run. IT IS NOW CHECKED BY A JOB — `.github/workflows/ci.yml`,
# in the `frontend` job, step "The interface image copies every workspace manifest".
#
# THE JOB COMPARES NAMES AND NOT COUNTS, AND THAT SENTENCE IS THE WHOLE OF WAVE 11'S REPAIR HERE.
# It compared counts until then, and this comment said so and called it a gate. W10-05's verifier
# broke the roster twice under an equal count and the step stayed green both times: deleting the
# `feature-ksql` line while duplicating the `kernel` one, and — worse, because the line still looks
# right — misspelling only its destination, `./packages/feature-kqsl/`. A count cannot see a
# substitution, and a substitution is what drift looks like once a roster is the length it should be.
#
# What the step derives now is three sorted lists — the package directories that exist, the package
# named on the LEFT of each COPY line, and the directory named on its RIGHT — and `diff` between
# them is the exit status rather than something printed inside a failure branch. Measured, on this
# tree, with each of the two mutations above re-applied by hand:
#
#   6d5 < feature-ksql / 9a9 > kernel        (the deleted line and its duplicate), exit 1
#   6c6 < feature-ksql --- > feature-kqsl    (the destination alone), exit 1
#
# and `frontend/packages: 11 packages, 11 manifests copied, every name matched`, exit 0, restored.
#
# AND IT COMPARED NAMES AND NOT FILES UNTIL WAVE 12, WHICH IS THE THIRD TIME THIS ROSTER HAS BEEN
# BROKEN UNDER A GREEN STEP. Both derivations reduced a line to the package name, so changing the
# FILE on the `packages/api` line below from `package.json` to `tsconfig.json` left eleven names on
# every side and every name equal: the step printed the sentence above and exited 0 — with
# `packages/api` shipping no manifest, which is the workspace pnpm resolves for a deleted line.
# Measured at the wave-12 open by making exactly that edit, and red on it now:
#
#   1c1 < api --- > COPY frontend/packages/api/tsconfig.json ./packages/api/   exit 1
#
# The source derivation is now anchored on `package\.json`, so a COPY line naming any
# other file does not reduce at all and appears WHOLE on the `>` side of the diff; the destination
# derivation cannot carry that anchor, because a destination names a directory and no file, and the
# step's fixture now holds `COPY frontend/packages/decoy/tsconfig.json ./packages/decoy/` as a line
# that must never reduce to a package name. Each line of the roster is three facts — the package,
# the file, and the directory pnpm will look in — and until wave 12 this was checked on two of them.
#
# The gate exists because the drift is otherwise invisible. What it costs when it is not benign: the
# install layer resolves a *different* workspace from the one the lockfile describes, so
# `--frozen-lockfile` is being asked a question about a tree that is not the tree, and the
# layer-cache win this ordering exists for is lost for the packages it omits. What it costs when it
# IS benign is nothing at all, which is why two waves of green builds never mentioned it.
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

RUN --mount=type=cache,id=pnpm-store,target=/pnpm-store \
    pnpm config set store-dir /pnpm-store && \
    pnpm install --frozen-lockfile

COPY frontend/ ./

# Typecheck before building. `vite build` transpiles without checking types, so without this a type
# error ships as a runtime error in somebody's browser.
RUN pnpm exec tsc --build --pretty false
RUN pnpm exec vite build --outDir /ui

# ------------------------------------------------------------------------------------------------
# Stage 2: serve it.
# ------------------------------------------------------------------------------------------------
#
# Unprivileged nginx: the stock image chowns its cache directories at start-up, which `cap_drop: ALL`
# forbids, and the container then restart-loops with an error about a directory nobody configured.
FROM nginxinc/nginx-unprivileged:1.27-alpine

COPY --from=build /ui /usr/share/nginx/html/ui

COPY deployment/frontend/entrypoint.sh /docker-entrypoint.d/40-kui-configure.sh
USER root
RUN chmod +x /docker-entrypoint.d/40-kui-configure.sh
USER nginx

EXPOSE 8082/tcp

# `/healthz` rather than `/ui/`: it answers without reading a file from disk, so it distinguishes
# "nginx is up" from "nginx is up and the interface is missing", which are different faults.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
  CMD ["/bin/sh", "-c", "wget -qO- http://127.0.0.1:8082/healthz >/dev/null || exit 1"]
