#!/bin/sh
#
# Writes the nginx configuration and finishes the interface's `index.html`, from the environment.
#
# Runs at container start rather than at build time, and that is the point: one image runs in every
# environment. Baking a gateway address or a mount point into the build means a separate image per
# environment, which is how a staging build reaches production.
#
# `nginx-unprivileged` runs everything in `/docker-entrypoint.d` before starting nginx, so this needs
# no ENTRYPOINT of its own.
set -eu

GATEWAY="${KUI_GATEWAY_URL:-http://kui:8080}"
BASE_PATH="${KUI_BASE_PATH:-}"
BUILD_VERSION="${KUI_BUILD_VERSION:-unknown}"

# A trailing slash on the mount point breaks both the `<base href>` and every `location`, and it is
# the natural way to write it. Strip it rather than refusing.
BASE_PATH="${BASE_PATH%/}"

# The image's filesystem is read-only, deliberately: nothing in a static file server has any business
# writing to it, and `read_only: true` in the compose file is a cheap, real containment property. So
# the two things that *are* per-deployment — the page's two markers and the server configuration —
# are rendered into tmpfs at start rather than edited in place.
SOURCE=/usr/share/nginx/html/ui/index.html
RENDERED=/tmp/kui/index.html
mkdir -p /tmp/kui
cp "$SOURCE" "$RENDERED"
INDEX="$RENDERED"

# ------------------------------------------------------------------------------------------------
# The page.
# ------------------------------------------------------------------------------------------------
#
# `frontend/index.html` carries two markers that the gateway substitutes per request when it serves
# the interface itself. Nothing substitutes them here, so this does — and it has to.
#
# Without the base href the application does not load past the root. `vite.config.ts` sets
# `base: "./"`, so every asset URL is relative, and a deep link such as
# `/ui/clusters/quickstart/topics` resolves `./assets/index-abc.js` against *that* directory —
# `/ui/clusters/quickstart/assets/index-abc.js`, a 404. The page renders blank, the console shows
# three 404s, and nothing says what is wrong.
#
# The bootstrap block is what tells the browser where the API is and which build it is looking at.
# `readBootstrap()` falls back to `/api/v1` and the word "dev" when it is absent, which is a
# reasonable default and a bad thing to ship: a bug report that says "dev" names no build.
if grep -q '<!--KUI_BASE_HREF-->' "$INDEX"; then
  sed -i "s|<!--KUI_BASE_HREF-->|<base href=\"${BASE_PATH}/ui/\">|" "$INDEX"
fi

if grep -q '<!--KUI_BOOTSTRAP-->' "$INDEX"; then
  BOOTSTRAP="{\"basePath\":\"${BASE_PATH}\",\"apiBase\":\"${BASE_PATH}/api/v1\",\"buildVersion\":\"${BUILD_VERSION}\"}"
  sed -i "s|<!--KUI_BOOTSTRAP-->|<script id=\"kui-bootstrap\" type=\"application/json\">${BOOTSTRAP}</script>|" "$INDEX"
fi

# Fail loudly if either substitution did not take. A blank page with three 404s is the failure this
# check exists to turn into a message.
grep -q "<base href=\"${BASE_PATH}/ui/\">" "$INDEX" || {
  echo "kui-frontend: the base href was not written into index.html" >&2
  exit 1
}

# ------------------------------------------------------------------------------------------------
# The server.
# ------------------------------------------------------------------------------------------------
cat > /etc/nginx/conf.d/default.conf <<NGINX
# Filters and cursor continuations travel in API query parameters. Keep operational request logs,
# but log only the normalized path so customer values and signed cursors never land in stdout.
log_format kui_no_query '\$remote_addr - \$remote_user [\$time_local] "\$request_method \$uri \$server_protocol" \$status \$body_bytes_sent "\$http_user_agent"';

server {
  listen 8082;
  server_tokens off;
  access_log /dev/stdout kui_no_query;

  root /usr/share/nginx/html;

  # Match KuiServer's 16 KiB request-line envelope: an 8 KiB browse cursor still
  # needs room for the path, the other query parameters and HTTP framing.
  large_client_header_buffers 4 16k;

  # Answered without touching the disk, so it says "nginx is up" and nothing more. Whether the
  # interface is actually present is a different question, and conflating them makes a missing
  # build look like a healthy container.
  location = /healthz {
    access_log off;
    add_header Content-Type text/plain;
    return 200 'ok';
  }

  # Everything the browser asks the gateway for. The REST surface and both server-sent-event
  # streams are under this one prefix, which is why the streaming settings live on this block.
  location ${BASE_PATH}/api/ {
    proxy_pass ${GATEWAY};
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto \$scheme;

    # Without these three a stream is buffered here and the browser sees nothing until the response
    # ends — which, for a live tail, is never.
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 1h;
  }

  # A deep link such as /ui/clusters/quickstart/topics is the router's, not a file. Answer it with
  # the application, exactly as the gateway's own single-page fallback does.
  location ${BASE_PATH}/ui/ {
    alias /usr/share/nginx/html/ui/;
    try_files \$uri \$uri/ ${BASE_PATH}/ui/index.html;
  }

  location = ${BASE_PATH}/ { return 302 ${BASE_PATH}/ui/; }

  # Vite writes a content hash into every asset filename, so an asset URL is immutable by
  # construction: if the content changes the URL changes. index.html is the one file whose name
  # is stable, so it is the one file that must never be cached.
  location ^~ ${BASE_PATH}/ui/assets/ {
    alias /usr/share/nginx/html/ui/assets/;
    expires 1y;
    add_header Cache-Control "public, immutable";
  }

  # The rendered page, from tmpfs rather than from the image: the markers are substituted at start
  # and the image itself stays read-only. The try_files directive above performs an internal redirect, which
  # re-runs location matching, so a deep link lands here too.
  location = ${BASE_PATH}/ui/index.html {
    alias ${RENDERED};
    add_header Cache-Control "no-store" always;

    # The headers live here rather than on the directory above it, and that is not a style choice.
    # try_files performs an *internal redirect*, so a request for /ui/ or for a deep link ends up
    # matching this block — and nginx's add_header does not inherit into a block that declares any
    # of its own. Set on the directory, they were dropped from every response that actually carried
    # the document. This is also the only response that needs them: the assets are hashed JavaScript
    # and CSS, and a policy on them protects nothing.
    #
    # The interface is same-origin and loads no third-party script, so the policy can be strict.
    # unsafe-inline is for styles only: the bundler emits a style element, and there is no inline
    # script — the bootstrap block is application/json, which is data and is never executed.
    add_header Content-Security-Policy "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'" always;
    add_header X-Content-Type-Options nosniff always;
    add_header Referrer-Policy no-referrer always;
    add_header X-Frame-Options DENY always;
  }

  gzip on;
  gzip_types text/plain text/css application/javascript application/json image/svg+xml;
  gzip_min_length 1024;
}
NGINX

echo "kui-frontend: serving ${BASE_PATH}/ui/, API proxied to ${GATEWAY}, build ${BUILD_VERSION}"
