#!/usr/bin/env bash
#
# Brings up a local Moat with the ACL view populated, end to end:
#
#   branch -> containers -> python deps -> migrations -> seed -> UI build
#          -> bundle worker -> flask -> policy evaluator -> browser
#
# Everything is idempotent, so re-running it is the normal way to pick up changes.
# Ctrl-C stops the local processes it started; the containers are left running.
#
# Usage:
#   ./dev_up.sh                 # full setup, then serve on :8000
#   MOAT_BRANCH=main ./dev_up.sh
#   MOAT_SKIP_EVAL=1 ./dev_up.sh   # skip the policy evaluator (no opa CLI needed)

set -euo pipefail

BRANCH="${MOAT_BRANCH:-worktree-acl-api-ui}"
PORT="${MOAT_PORT:-8000}"
OPA_URL="${MOAT_OPA_URL:-http://localhost:8181}"
LOG_DIR="${MOAT_LOG_DIR:-/tmp/moat-dev}"

FLASK_PID=""
WORKER_PID=""

say() { printf '\n\033[1;34m==>\033[0m %s\n' "$1"; }
warn() { printf '\033[1;33m warning:\033[0m %s\n' "$1"; }
die() { printf '\033[1;31m error:\033[0m %s\n' "$1" >&2; exit 1; }

cleanup() {
    [ -n "$WORKER_PID" ] && kill "$WORKER_PID" 2>/dev/null || true
    [ -n "$FLASK_PID" ] && kill "$FLASK_PID" 2>/dev/null || true
}
trap cleanup INT TERM EXIT

# ---------------------------------------------------------------- 1. the branch
# A branch cannot be checked out twice, so if it already lives in a worktree this
# has to run from there rather than trying to switch the main checkout to it.
say "Checking out $BRANCH"
command -v git >/dev/null || die "git is not installed"
git rev-parse --git-dir >/dev/null 2>&1 || die "not inside a git repository"

git fetch --quiet origin "$BRANCH" 2>/dev/null || warn "could not fetch $BRANCH from origin"

branch_worktree="$(git worktree list --porcelain \
    | awk -v b="refs/heads/$BRANCH" '/^worktree /{w=$2} /^branch /{if ($2==b) print w}')"

if [ -n "$branch_worktree" ]; then
    cd "$branch_worktree"
    echo "    $BRANCH is checked out at $branch_worktree"
elif [ "$(git rev-parse --abbrev-ref HEAD)" = "$BRANCH" ]; then
    cd "$(git rev-parse --show-toplevel)"
else
    cd "$(git rev-parse --show-toplevel)"
    git switch "$BRANCH" 2>/dev/null || git switch -c "$BRANCH" --track "origin/$BRANCH"
fi

# every config path in the repo is relative to the root, so stay here
ROOT="$PWD"
echo "    working in $ROOT"

# ------------------------------------------------------------- 2. the containers
# podman first - it is what the dev setup documents - then fall back to docker.
if command -v podman-compose >/dev/null; then
    COMPOSE="podman-compose"
elif command -v docker >/dev/null && docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null; then
    COMPOSE="docker-compose"
else
    die "neither podman-compose nor docker compose is installed"
fi

# compose derives its project name from the directory, so running from a worktree
# would build a second, port-clashing stack. Pin it to the one name instead.
export COMPOSE_PROJECT_NAME="${MOAT_PROJECT:-moat}"

# Only the services this flow needs. Everything else is either irrelevant here
# (trino, lldap, keycloak, mysql) or built from this repo (moat, moat_worker,
# mock_api) - and 'up' would rebuild those images, which takes minutes and buys
# nothing, as Moat itself is run from the venv below. Both of these are prebuilt.
SERVICES="${MOAT_SERVICES:-postgres opa}"

say "Starting containers with $COMPOSE ($SERVICES)"
# shellcheck disable=SC2086
$COMPOSE up -d $SERVICES

say "Waiting for postgres on localhost:5432"
for _ in $(seq 1 60); do
    if nc -z localhost 5432 2>/dev/null; then break; fi
    sleep 1
done
nc -z localhost 5432 2>/dev/null || die "postgres never came up on localhost:5432"

# --------------------------------------------------------------- 3. python deps
# In a worktree the venv and node_modules of the main checkout are reused rather
# than built again - both are gitignored, so a worktree starts without them.
MAIN_ROOT="$(cd "$(dirname "$(git rev-parse --git-common-dir)")" && pwd)"

say "Preparing the virtualenv"
if [ -d venv ]; then
    VENV="$ROOT/venv"
elif [ -d "$MAIN_ROOT/venv" ]; then
    VENV="$MAIN_ROOT/venv"
    echo "    reusing $VENV"
else
    python3.12 -m venv venv || die "python3.12 is required (brew install python@3.12)"
    VENV="$ROOT/venv"
fi
# shellcheck disable=SC1091
source "$VENV/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

export PYTHONPATH="$ROOT/moat/src"
export FLASK_SECRET_KEY="${FLASK_SECRET_KEY:-dev-secret}"

mkdir -p "$LOG_DIR"

# --------------------------------------------------------- 4. schema and seeding
say "Running migrations"
alembic -c moat/alembic.ini upgrade head

say "Seeding principals and resources"
python moat/src/_scripts/seed_db.py >"$LOG_DIR/seed.log" 2>&1
echo "    seeded (log: $LOG_DIR/seed.log)"

# ----------------------------------------------------------------- 5. the UI
say "Building the UI"
command -v npm >/dev/null || die "npm is not installed (brew install node)"
if [ ! -e moat/ui/node_modules ]; then
    if [ -d "$MAIN_ROOT/moat/ui/node_modules" ]; then
        ln -s "$MAIN_ROOT/moat/ui/node_modules" moat/ui/node_modules
        echo "    linked node_modules from $MAIN_ROOT"
    else
        npm --prefix moat/ui install
    fi
fi
npm --prefix moat/ui run build

# ------------------------------------------------------- 6. flask, bundle, ACL

# without this an already running server would answer the health check below and
# the script would report success while serving someone else's code
if nc -z localhost "$PORT" 2>/dev/null; then
    die "something is already listening on port $PORT - stop it, or set MOAT_PORT"
fi

say "Starting Moat on http://localhost:$PORT"
flask --app moat.src.app run --debug --port "$PORT" >"$LOG_DIR/flask.log" 2>&1 &
FLASK_PID=$!

for _ in $(seq 1 60); do
    if curl -sf "http://localhost:$PORT/api/v1/healthcheck" >/dev/null; then break; fi
    sleep 1
done
curl -sf "http://localhost:$PORT/api/v1/healthcheck" >/dev/null \
    || die "Moat did not start - see $LOG_DIR/flask.log"
echo "    up (log: $LOG_DIR/flask.log)"

if [ -n "${MOAT_SKIP_EVAL:-}" ]; then
    warn "MOAT_SKIP_EVAL set - skipping the bundle and policy evaluator"
elif ! command -v opa >/dev/null; then
    warn "the 'opa' CLI is not installed, so no bundle can be built and the ACL"
    warn "view will be empty. Install it with 'brew install opa' and re-run,"
    warn "or set MOAT_SKIP_EVAL=1 to silence this."
else
    # the worker builds the OPA bundle and stores it; flask then serves it to OPA,
    # which polls every 5-10s. The evaluator needs that bundle loaded to answer.
    say "Starting the bundle worker"
    python -m cli.src.cli start-worker >"$LOG_DIR/worker.log" 2>&1 &
    WORKER_PID=$!

    say "Waiting for OPA to load the bundle"
    for _ in $(seq 1 60); do
        if curl -sf "$OPA_URL/v1/policies" 2>/dev/null | grep -q '"id"'; then break; fi
        sleep 2
    done
    if curl -sf "$OPA_URL/v1/policies" 2>/dev/null | grep -q '"id"'; then
        echo "    bundle loaded"
        say "Evaluating policy to populate the ACL"
        python -m cli.src.cli evaluate-policy
    else
        warn "OPA never loaded a bundle - see $LOG_DIR/worker.log."
        warn "The ACL view will be empty until 'python -m cli.src.cli evaluate-policy' succeeds."
    fi
fi

# ------------------------------------------------------------------- 7. browser
say "Ready"
echo "    UI          http://localhost:$PORT/"
echo "    ACL view    http://localhost:$PORT/acl/"
echo "    ACL API     http://localhost:$PORT/api/acl/v1/grants"
echo "    OpenAPI     http://localhost:$PORT/openapi.json"
echo
echo "    Ctrl-C stops Moat and the worker. The containers keep running:"
echo "      $COMPOSE down"

if [ -z "${MOAT_NO_OPEN:-}" ] && command -v open >/dev/null; then
    open "http://localhost:$PORT/acl/"
fi

wait "$FLASK_PID"
