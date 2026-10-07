#!/usr/bin/env bash
#
# The demonstration environment: three Kafka clusters and one KUI that already knows all three.
#
#   deployment/demo/demo.sh                start everything and print the URL
#   deployment/demo/demo.sh down           stop everything and remove it, volumes included
#   deployment/demo/demo.sh stop   <what>  stop one cluster, or one broker, and leave the rest up
#   deployment/demo/demo.sh start  <what>  start it again
#   deployment/demo/demo.sh logs           follow every container's log
#   deployment/demo/demo.sh status         what is running
#
# `<what>` is one of:
#
#   dev           the single-broker development cluster
#   prod          the whole three-broker production cluster
#   prod-broker   ONE broker of the production cluster -- the under-replication demonstration
#   secured       the SASL/TLS cluster
#
# The only thing this needs installed is Docker (with the Compose plugin, which Docker Desktop and
# every current Docker Engine package include). No Java, no Mill, no Scala, no OpenSSL: if the KUI
# image is missing this script builds it inside a container, and if the demonstration certificate
# authority is missing it generates that in a container too.
#
# Ports, when any of the defaults is already taken on your machine:
#
#   KUI_PORT=28080 KUI_DEMO_FRONTEND_PORT=28090 KUI_DEMO_DEV_PORT=29092 deployment/demo/demo.sh
#
# Pass the same variables to `down`, or not -- teardown does not care which ports were used.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/../.." && pwd)"
COMPOSE_FILE="${HERE}/docker-compose.demo.yml"
CERTS_DIR="${REPO_ROOT}/deployment/secured/certs"

KUI_VERSION="${KUI_VERSION:-0.1.0}"
KUI_IMAGE="kui-allinone:${KUI_VERSION}"

# The defaults are deliberately NOT 8080 and 9092. The quickstart owns those, and somebody who wants
# to see both at once should not have to think about it.
KUI_PORT="${KUI_PORT:-18080}"
# The interface's port. It is what a person opens; KUI's port above is the API, published only so a
# curl against it is possible -- the browser reaches it through the frontend container, which
# proxies /api so that the two share an origin (ADR-019 and the cookie).
FRONTEND_PORT="${KUI_DEMO_FRONTEND_PORT:-18090}"
DEV_PORT="${KUI_DEMO_DEV_PORT:-19092}"
PROD_PORT_1="${KUI_DEMO_PROD_PORT_1:-19093}"
PROD_PORT_2="${KUI_DEMO_PROD_PORT_2:-19094}"
PROD_PORT_3="${KUI_DEMO_PROD_PORT_3:-19095}"

export KUI_VERSION KUI_PORT
export KUI_DEMO_FRONTEND_PORT="${FRONTEND_PORT}"
export KUI_DEMO_DEV_PORT="${DEV_PORT}"
export KUI_DEMO_PROD_PORT_1="${PROD_PORT_1}"
export KUI_DEMO_PROD_PORT_2="${PROD_PORT_2}"
export KUI_DEMO_PROD_PORT_3="${PROD_PORT_3}"

# How much memory to insist on. Measured on an idle stack, the containers hold about 3 GB between
# them -- roughly 400 MiB per broker with its 512 MiB heap cap, 540 MiB for KUI, about 120 MiB for
# each of the three demonstration consumers, and a rounding error for the interface's nginx. 6 GB is
# that plus room for the JVM heaps to actually reach their cap while you use the thing. Stated
# rather than discovered, because the failure mode when it is not there is a broker killed by the
# kernel's out-of-memory reaper mid-run, which surfaces as an unexplained cluster going stale ten
# minutes in.
readonly MEMORY_NEEDED_GB=6

compose() {
  docker compose --project-directory "${HERE}" -f "${COMPOSE_FILE}" "$@"
}

say() { printf '%s\n' "$*"; }
die() { printf '%s\n' "$*" >&2; exit 1; }

# The long-lived services of each cluster: the ones `stop` and `start` act on. The one-shot seed
# containers are left out on purpose -- they have already exited, and starting a cluster again does
# not need re-seeding, because the data is still in the broker that was only stopped.
services_for() {
  case "$1" in
    dev)         echo "kafka-dev consumer-dev" ;;
    prod)        echo "kafka-prod-1 kafka-prod-2 kafka-prod-3 consumer-prod" ;;
    prod-broker) echo "kafka-prod-3" ;;
    secured)     echo "kafka-secured consumer-secured" ;;
    *)           return 1 ;;
  esac
}

require_docker() {
  command -v docker >/dev/null 2>&1 \
    || die "Docker is not installed, or not on this shell's PATH. It is the only prerequisite: https://docs.docker.com/get-docker/"
  docker info >/dev/null 2>&1 \
    || die "Docker is installed but not running, or this user cannot talk to it. Start Docker and try again."
  docker compose version >/dev/null 2>&1 \
    || die "This Docker has no 'compose' plugin. Install docker-compose-plugin, or use Docker Desktop, which includes it."
}

# Three clusters and five brokers is real memory. Say so before the run rather than letting a broker
# be killed halfway through it.
check_memory() {
  local bytes
  bytes="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
  [[ "${bytes}" =~ ^[0-9]+$ ]] || return 0
  [[ "${bytes}" -gt 0 ]] || return 0
  local gb=$(( bytes / 1024 / 1024 / 1024 ))
  if [[ "${gb}" -lt "${MEMORY_NEEDED_GB}" ]]; then
    say "Docker has ${gb} GB of memory available and this stack wants about ${MEMORY_NEEDED_GB} GB."
    say ""
    say "  Three clusters is five Kafka brokers plus KUI: about 3 GB resident, and more while it is"
    say "  working. With less, a broker is likely to be killed part-way through the run, which looks"
    say "  like a cluster going stale for no reason. Two ways out:"
    say ""
    say "    * give Docker more memory (Docker Desktop: Settings -> Resources -> Memory), or"
    say "    * run the single-broker quickstart instead: deployment/quickstart/quickstart.sh"
    say ""
    say "Continuing anyway in case you know better than this check does."
    say ""
  fi
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

# Whether any container of *this* stack already exists, running or stopped.
#
# `compose ps -q --all` lists the ids Compose knows about for this project, so it answers about our
# own containers and nobody else's.
stack_exists() {
  [ -n "$(compose ps -q --all 2>/dev/null)" ]
}

check_ports() {
  local blocked=()

  # Running `demo.sh` again while the demonstration is up is a normal thing to do -- it is how you
  # bring back a cluster you stopped, and how you pick up a rebuilt KUI image. In that situation the
  # only thing listening on these ports is this stack itself, and telling the user to "choose free
  # ports" would be advice that breaks what is already working. Compose is perfectly capable of
  # re-converging containers it owns, so hand the job to it.
  if stack_exists; then
    return
  fi

  port_in_use "${FRONTEND_PORT}" && blocked+=("${FRONTEND_PORT} (the interface, override with KUI_DEMO_FRONTEND_PORT)")
  port_in_use "${KUI_PORT}"      && blocked+=("${KUI_PORT} (the KUI API, override with KUI_PORT)")
  port_in_use "${DEV_PORT}"      && blocked+=("${DEV_PORT} (development broker, override with KUI_DEMO_DEV_PORT)")
  port_in_use "${PROD_PORT_1}"   && blocked+=("${PROD_PORT_1} (production broker 1, override with KUI_DEMO_PROD_PORT_1)")
  port_in_use "${PROD_PORT_2}"   && blocked+=("${PROD_PORT_2} (production broker 2, override with KUI_DEMO_PROD_PORT_2)")
  port_in_use "${PROD_PORT_3}"   && blocked+=("${PROD_PORT_3} (production broker 3, override with KUI_DEMO_PROD_PORT_3)")
  if [ "${#blocked[@]}" -gt 0 ]; then
    say "Something is already listening on:"
    printf '  %s\n' "${blocked[@]}"
    say ""
    say "Choose free ports and pass them in, for example:"
    say "  KUI_PORT=$((KUI_PORT + 10000)) \\"
    say "  KUI_DEMO_FRONTEND_PORT=$((FRONTEND_PORT + 10000)) \\"
    say "  KUI_DEMO_DEV_PORT=$((DEV_PORT + 10000)) \\"
    say "  KUI_DEMO_PROD_PORT_1=$((PROD_PORT_1 + 10000)) \\"
    say "  KUI_DEMO_PROD_PORT_2=$((PROD_PORT_2 + 10000)) \\"
    say "  KUI_DEMO_PROD_PORT_3=$((PROD_PORT_3 + 10000)) \\"
    say "  ${0}"
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
  say "  EXPECT SEVERAL MINUTES the first time. It happens once: the image is kept, and later runs"
  say "  start immediately."
  say ""

  # The quickstart's Dockerfile, unchanged. There is one way to build this image and this is not a
  # second one.
  docker build \
    --file "${REPO_ROOT}/deployment/quickstart/Dockerfile" \
    --tag "${KUI_IMAGE}" \
    --progress plain \
    "${REPO_ROOT}" \
    || die "The KUI image did not build. The build output above says why."

  say ""
  say "Built ${KUI_IMAGE}."
  say ""
}

# The secured cluster's certificate authority. ../secured/generate-certs.sh writes it into a
# git-ignored directory using a throwaway container, so it is absent on a fresh clone and has to be
# made before the stack starts -- which is this script's job and not the reader's.
ensure_certs() {
  if [ -r "${CERTS_DIR}/broker.keystore.p12" ] && [ -r "${CERTS_DIR}/kafka-ca.p12" ]; then
    return
  fi
  say "Generating the secured cluster's demonstration certificate authority (once, about ten seconds)."
  say ""
  "${REPO_ROOT}/deployment/secured/generate-certs.sh" \
    || die "The certificates could not be generated. The output above says why."
  say ""
}

# And the interface, which is a second container. Reporting the API as ready and then handing over a
# URL that answers nothing is the worst moment to be imprecise: somebody seeing KUI for the first
# time reads a blank page as the product being broken.
wait_for_frontend() {
  local url="http://localhost:${FRONTEND_PORT}/healthz"
  local deadline=$((SECONDS + 120))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if curl -fsS -o /dev/null "${url}" 2>/dev/null; then
      return 0
    fi
    if ! command -v curl >/dev/null 2>&1; then
      case "$(docker inspect -f '{{.State.Health.Status}}' kui-demo-frontend 2>/dev/null || echo none)" in
        healthy) return 0 ;;
      esac
    fi
    sleep 2
  done
  return 1
}

wait_for_kui() {
  local url="http://localhost:${KUI_PORT}/api/v1/health/ready"
  local deadline=$((SECONDS + 240))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if curl -fsS -o /dev/null "${url}" 2>/dev/null; then
      return 0
    fi
    # No curl on this machine? Fall back to asking Docker what it thinks of the container's own
    # health check, which is the same check from the inside.
    if ! command -v curl >/dev/null 2>&1; then
      case "$(docker inspect -f '{{.State.Health.Status}}' kui-demo-kui 2>/dev/null || echo none)" in
        healthy) return 0 ;;
      esac
    fi
    sleep 2
  done
  return 1
}

up() {
  require_docker
  check_memory
  check_ports
  ensure_image
  ensure_certs

  say "Starting three Kafka clusters, seeding each of them, and starting KUI and its interface."
  say "  Each cluster is waited for until it can genuinely serve a client, not merely until its"
  say "  containers have started -- and the three-broker cluster is waited for until its brokers"
  say "  have found each other, which is a stronger condition than one broker answering. Expect"
  say "  roughly a minute."
  say ""

  compose up -d --wait --wait-timeout 420 \
    || die "Startup failed. 'deployment/demo/demo.sh logs' shows what happened."

  if ! wait_for_kui; then
    die "KUI started but never became ready. 'deployment/demo/demo.sh logs' shows why."
  fi

  if ! wait_for_frontend; then
    die "The interface did not come up. 'deployment/demo/demo.sh logs' shows why."
  fi

  say ""
  say "  KUI is running:  http://localhost:${FRONTEND_PORT}/ui/"
  say "  the API is at:   http://localhost:${KUI_PORT}/api/v1"
  say ""
}

down() {
  require_docker
  # -v removes any named volume, --remove-orphans removes containers left behind by an older version
  # of this file. Between them nothing of this stack survives except the images.
  compose down -v --remove-orphans
  say "Removed. No container, network or volume from the demonstration is left."
  say "The images stay, because re-downloading them next time would waste your time:"
  say "  docker image rm ${KUI_IMAGE} apache/kafka:4.3.1"
}

usage_targets() {
  say "Say which one:"
  say "  dev           the single-broker development cluster"
  say "  prod          the whole three-broker production cluster"
  say "  prod-broker   one broker of the production cluster, leaving two"
  say "  secured       the SASL/TLS cluster"
}

# Stopping a cluster is the demonstration this whole environment is shaped around: KUI must keep
# working, keep serving the other two clusters, and report the missing one rather than breaking. The
# containers are stopped and not removed, so their data survives and `start` brings back the same
# cluster rather than an empty one.
stop_target() {
  require_docker
  local target="${1:-}" services
  services="$(services_for "${target}")" || { say "Nothing here is called '${target}'."; say ""; usage_targets; exit 1; }

  # shellcheck disable=SC2086
  compose stop ${services}
  say ""
  case "${target}" in
    prod-broker)
      say "Stopped one of the three production brokers."
      say "  The cluster keeps serving: its two survivors elect new leaders for that broker's"
      say "  partitions. Topics with three replicas now have two in sync, so KUI shows them as"
      say "  under-replicated, and the ones with min.insync.replicas=2 are at their limit -- one more"
      say "  broker and they would refuse writes."
      ;;
    *)
      say "Stopped the ${target} cluster. The other two are still running."
      say "  KUI is still up and still serving them at full speed. Within about a minute -- its next"
      say "  background scrape -- the stopped cluster is marked STALE and keeps showing the last data"
      say "  it had, rather than disappearing or taking the rest of the interface down with it. That"
      say "  is the behaviour this whole environment exists to show."
      ;;
  esac
  say ""
  say "Bring it back with:  ${0} start ${target}"
  say ""
}

start_target() {
  require_docker
  local target="${1:-}" services
  services="$(services_for "${target}")" || { say "Nothing here is called '${target}'."; say ""; usage_targets; exit 1; }

  # shellcheck disable=SC2086
  compose start ${services}
  say ""
  say "Started ${target} again, with the data it had before. KUI picks it up on its next refresh,"
  say "within about half a minute -- or immediately if you press refresh on the cluster."
  say ""
}

case "${1:-up}" in
  up|"")   up ;;
  down)    down ;;
  stop)    stop_target "${2:-}" ;;
  start)   start_target "${2:-}" ;;
  logs)    require_docker; compose logs -f ;;
  status)  require_docker; compose ps ;;
  *)
    die "Usage: ${0} [up|down|status|logs|stop <what>|start <what>]"
    ;;
esac
