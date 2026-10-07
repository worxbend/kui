#!/usr/bin/env bash
#
# The quickstart: one command that leaves you with KUI running against a Kafka broker that has data
# in it.
#
#   deployment/quickstart/quickstart.sh          start everything and print the URL
#   deployment/quickstart/quickstart.sh --with-auth   the same, but with a login and two roles
#   deployment/quickstart/quickstart.sh down     stop everything and remove it, volumes included
#   deployment/quickstart/quickstart.sh logs     follow the logs
#   deployment/quickstart/quickstart.sh status   what is running
#
# The only thing this needs installed is Docker (with the Compose plugin, which Docker Desktop and
# every current Docker Engine package include). No Java, no Mill, no Scala: if the KUI image is not
# on the machine, this script builds it inside a container.
#
# Ports, when 8090, 8080 or 9092 are already taken on your machine:
#
#   KUI_FRONTEND_PORT=18090 KUI_PORT=18080 KUI_QUICKSTART_KAFKA_PORT=19092 \
#     deployment/quickstart/quickstart.sh
#
# Pass the same variables to `down`, or not — teardown does not care which ports were used.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
COMPOSE_FILE="${HERE}/docker-compose.quickstart.yml"

# The interface, which is a separate image and a separate compose file (deployment/frontend). The
# quickstart brings up both, because "run KUI" means the whole product to somebody trying it for the
# first time — the split matters to whoever releases the two halves, not to whoever is opening a
# browser.
FRONTEND_FILE="${HERE}/../frontend/docker-compose.frontend.yml"
AUTH_FILE="${HERE}/docker-compose.auth.yml"

# `--with-auth` swaps KUI's configuration file for one that has `kui.auth` and `kui.rbac` filled in.
# It is a Compose override rather than a second topology: the broker, the data and the registry are
# the same, so the only difference between the two runs is the login. It is accepted before the
# subcommand so that `--with-auth down` tears the same thing down.
WITH_AUTH=false
if [[ "${1:-}" == "--with-auth" ]]; then
  WITH_AUTH=true
  shift
fi

KUI_VERSION="${KUI_VERSION:-0.1.0}"
KUI_IMAGE="kui-allinone:${KUI_VERSION}"
KUI_PORT="${KUI_PORT:-8080}"

# The interface's port. It is what a person opens; the gateway's port above is the API, and is
# published only so that a `curl` against it is possible — the browser reaches it through the
# frontend container, which proxies `/api` so that the two share an origin (ADR-019 and the cookie).
KUI_FRONTEND_PORT="${KUI_FRONTEND_PORT:-8090}"
export KUI_FRONTEND_PORT
KAFKA_PORT="${KUI_QUICKSTART_KAFKA_PORT:-9092}"

# The Connect worker's REST port. It is published because the quickstart now runs a connector on
# that worker, and reading the worker's own answer beside KUI's screen is how anybody checks the
# screen is honest; the default matches Kafka Connect's own.
CONNECT_PORT="${KUI_QUICKSTART_CONNECT_PORT:-8083}"

# The ksqlDB server's REST port, published for the same reason and with ksqlDB's own default.
KSQL_PORT="${KUI_QUICKSTART_KSQL_PORT:-8088}"

export KUI_VERSION KUI_PORT
export KUI_QUICKSTART_KAFKA_PORT="${KAFKA_PORT}"
export KUI_QUICKSTART_CONNECT_PORT="${CONNECT_PORT}"
export KUI_QUICKSTART_KSQL_PORT="${KSQL_PORT}"

compose() {
  if [ "${WITH_AUTH}" = true ]; then
    docker compose --project-directory "${HERE}" -f "${COMPOSE_FILE}" -f "${FRONTEND_FILE}" -f "${AUTH_FILE}" "$@"
  else
    docker compose --project-directory "${HERE}" -f "${COMPOSE_FILE}" -f "${FRONTEND_FILE}" "$@"
  fi
}

say()  { printf '%s\n' "$*"; }
die()  { printf '%s\n' "$*" >&2; exit 1; }

require_docker() {
  command -v docker >/dev/null 2>&1 \
    || die "Docker is not installed, or not on this shell's PATH. It is the only prerequisite: https://docs.docker.com/get-docker/"
  docker info >/dev/null 2>&1 \
    || die "Docker is installed but not running, or this user cannot talk to it. Start Docker and try again."
  docker compose version >/dev/null 2>&1 \
    || die "This Docker has no 'compose' plugin. Install docker-compose-plugin, or use Docker Desktop, which includes it."
}

# A port already in use produces a Compose error that names a container and a port and does not
# suggest a way out. Checking first lets the message say what to do instead.
port_in_use() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "[:.]${port}\$"
  elif command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1
  else
    return 1
  fi
}

# A port the quickstart's OWN containers already publish is not a conflict, and until this function
# existed the check could not tell the difference. Running this script twice -- which is how a
# running quickstart picks up a changed Compose file, because `up` reconciles rather than
# replaces -- refused with "something is already listening on 8080", and the something was
# the quickstart. Three waves of this plan recorded the port check firing and none of them recorded
# that it fires on the quickstart itself.
#
# The project's own ports are asked of Compose rather than derived from the variables above, so that
# a run started under `KUI_PORT=18080` is recognised as this project's by the port it actually
# published and not by the port this shell happens to be holding in a variable.
published_by_this_project() {
  local port="$1"
  compose ps --format json 2>/dev/null |
    grep -o '"PublishedPort":[0-9]*' |
    grep -qx "\"PublishedPort\":${port}"
}

check_ports() {
  local blocked=()
  port_in_use "${KUI_PORT}" && ! published_by_this_project "${KUI_PORT}" \
    && blocked+=("${KUI_PORT} (KUI, override with KUI_PORT)")
  port_in_use "${KAFKA_PORT}" && ! published_by_this_project "${KAFKA_PORT}" \
    && blocked+=("${KAFKA_PORT} (Kafka, override with KUI_QUICKSTART_KAFKA_PORT)")
  # The Connect worker's REST port, published since the quickstart started running a connector on
  # it. Checked here for the reason the two above are: a bound port is a Compose error naming a
  # container, and this message can say which variable moves it instead.
  port_in_use "${CONNECT_PORT}" && ! published_by_this_project "${CONNECT_PORT}" \
    && blocked+=("${CONNECT_PORT} (Kafka Connect, override with KUI_QUICKSTART_CONNECT_PORT)")
  port_in_use "${KSQL_PORT}" && ! published_by_this_project "${KSQL_PORT}" \
    && blocked+=("${KSQL_PORT} (ksqlDB, override with KUI_QUICKSTART_KSQL_PORT)")
  if [ "${#blocked[@]}" -gt 0 ]; then
    say "Something is already listening on:"
    printf '  %s\n' "${blocked[@]}"
    say ""
    say "Choose free ports and pass them in, for example:"
    say "  KUI_PORT=$((KUI_PORT + 10000)) KUI_QUICKSTART_KAFKA_PORT=$((KAFKA_PORT + 10000)) ${0}"
    exit 1
  fi
}

# The image is built only when it is missing, so a second run starts in seconds. Anyone with a JDK
# who would rather have the reproducible image build.mill produces can run
# `./mill deployment.docker.allinone.docker.build` first; this will then find it and skip the build.
ensure_image() {
  if docker image inspect "${KUI_IMAGE}" >/dev/null 2>&1; then
    return
  fi

  say "The KUI image ${KUI_IMAGE} is not on this machine, so it has to be built."
  say ""
  say "  This compiles KUI from source inside a container, so that a machine with only Docker"
  say "  installed is enough. It downloads a JDK image, the Mill build tool and every Scala"
  say "  dependency, then compiles the project."
  say ""
  say "  EXPECT SEVERAL MINUTES the first time — around two on a fast connection, longer on a"
  say "  slow one. It happens once: the image is kept, and later runs start immediately."
  say ""
  say "Building now. The build's own output follows, so you can see it is not stuck."
  say ""

  docker build \
    --file "${HERE}/Dockerfile" \
    --tag "${KUI_IMAGE}" \
    --progress plain \
    "${REPO_ROOT}" \
    || die "The KUI image did not build. The build output above says why."

  say ""
  say "Built ${KUI_IMAGE}."
  say ""
}

wait_for_frontend() {
  local url="http://localhost:${KUI_FRONTEND_PORT}/healthz"
  local deadline=$((SECONDS + 120))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if curl -fsS -o /dev/null "${url}" 2>/dev/null; then
      return 0
    fi
    if ! command -v curl >/dev/null 2>&1; then
      case "$(docker inspect -f '{{.State.Health.Status}}' kui-quickstart-frontend 2>/dev/null || echo none)" in
        healthy) return 0 ;;
      esac
    fi
    sleep 2
  done
  return 1
}

wait_for_kui() {
  local url="http://localhost:${KUI_PORT}/api/v1/health/ready"
  local deadline=$((SECONDS + 180))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if curl -fsS -o /dev/null "${url}" 2>/dev/null; then
      return 0
    fi
    # No curl on this machine? Fall back to asking Docker what it thinks of the container's own
    # health check, which is the same check from the inside.
    if ! command -v curl >/dev/null 2>&1; then
      case "$(docker inspect -f '{{.State.Health.Status}}' kui-quickstart-kui 2>/dev/null || echo none)" in
        healthy) return 0 ;;
      esac
    fi
    sleep 2
  done
  return 1
}

up() {
  require_docker
  check_ports
  ensure_image

  say "Starting Kafka, seeding it, and starting KUI."
  say "  Kafka is waited for until it can actually serve metadata, not merely until it has started,"
  say "  so this pauses for around half a minute before the seed step runs."
  say ""

  # No `--wait`. It looks like the right flag and it races the seeders: `avro-seed` is a one-shot
  # container that does its work and exits 0, and `--wait` reports an exited container as a failure
  # unless its health check happened to pass first. Which of the two wins depends on how long
  # everything else took, so adding one service to this stack was enough to turn a green start-up
  # into "container kui-quickstart-avro-seed exited (0)" and a script that stopped.
  #
  # The two waits below are the ones that mean something anyway: the API answering `health/ready`,
  # and the interface answering. Both say what they were waiting for when they give up.
  compose up -d \
    || die "Startup failed. 'deployment/quickstart/quickstart.sh logs' shows what happened."

  if ! wait_for_kui; then
    die "KUI started but never became ready. 'deployment/quickstart/quickstart.sh logs' shows why."
  fi

  # And the interface, which is a second container. Reporting the gateway as ready and then handing
  # over a URL that answers nothing is the worst moment to be imprecise: somebody trying KUI for the
  # first time reads a blank page as the product being broken.
  if ! wait_for_frontend; then
    die "The interface did not come up. 'deployment/quickstart/quickstart.sh logs' shows why."
  fi

  say ""
  say "  KUI is running:  http://localhost:${KUI_FRONTEND_PORT}/ui/"
  say "  the API is at:   http://localhost:${KUI_PORT}/api/v1"
  if [ "${WITH_AUTH}" = true ]; then
    say ""
    say "  It asks you to sign in. Two accounts, both spelled out in kui-quickstart-auth.yaml:"
    say ""
    # "everything on this cluster" until wave 12, which was false and measurable: this role grants
    # TOPIC, CONSUMER, CLUSTERCONFIG and SCHEMA, and signed in as admin a connector `pause` answers
    # 403 and ksqlDB answers 403. kui-quickstart-auth.yaml carries the measurement and the decision.
    say "    admin  / quickstart-admin    may do everything to this cluster's topics, groups,"
    say "                                 schemas and connection settings"
    say "    viewer / quickstart-viewer   may look at topics and read records, and nothing else"
    say ""
    say "  Sign in as viewer to see the difference: the create, edit, add-partitions, empty and"
    say "  delete controls are still on screen, greyed out, and each says why when you point at"
    say "  it. Nothing is hidden, so you can see what the account cannot do as well as what it can."
  fi
  say ""
}

down() {
  require_docker
  # -v removes the named volumes, --remove-orphans removes containers left by an older version of
  # this file. Between them, nothing of this quickstart survives except the two images, which
  # `docker image rm kui-allinone:${KUI_VERSION} apache/kafka:4.3.1` removes if you want the disk
  # back.
  compose down -v --remove-orphans
  say "Removed. Nothing from the quickstart is left running or stored."
}

case "${1:-up}" in
  up|"")   up ;;
  down)    down ;;
  logs)    require_docker; compose logs -f ;;
  status)  require_docker; compose ps ;;
  *)
    die "Usage: ${0} [--with-auth] [up|down|logs|status]"
    ;;
esac
