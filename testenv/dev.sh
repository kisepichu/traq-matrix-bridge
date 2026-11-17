#!/usr/bin/env bash
# Development environment for traq-matrix-bridge: traQ + Synapse in docker, the bridge on the host.
# See docs/development.md. Requires bash, curl, jq, docker and go.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DEV=$ROOT/testenv/.dev
COMPOSE=(docker compose -p tmb -f "$ROOT/testenv/compose.yaml" --profile matrix)

MATRIX=http://localhost:8008
TRAQ=http://localhost:3080
SERVER_NAME=localhost
DEV_USER=dev
DEV_PASSWORD=dev-password
BOT_MXID=@traqbot:$SERVER_NAME
# The bridge listens on all interfaces because Synapse (in docker) reaches it through host.docker.internal.
AS_PORT=29338

die() { echo "dev.sh: $*" >&2; exit 1; }

usage() {
  cat <<'EOF'
usage: testenv/dev.sh <command> [args]

  up                                  start traQ and Synapse, generate the bridge config and registrations
  down                                stop everything and delete testenv/.dev
  setup                               create the traQ OAuth client and users, and the Matrix user @dev:localhost
  bridge                              build the bridge and run it in the foreground
  bot <text>                          send text to the bridge bot (management room) as @dev
  tail [room]                         show the last 20 events of a room (default: management room)
  rooms                               list the rooms @dev is in
  send <room> <text>                  send text to a room as @dev
  traq-login <user> <authorize-url>   approve the OAuth authorize URL as a traQ user, print the redirect URL
  traq <user> <METHOD> <path> [json]  call the traQ REST API (/api/v3 is prepended to path) as a traQ user
EOF
}

# --- helpers ---------------------------------------------------------------

need_dev() { [[ -d $DEV ]] || die "testenv/.dev does not exist; run 'testenv/dev.sh up' first"; }

urlencode() { jq -rn --arg s "$1" '$s | @uri'; }

random_hex() { od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'; }

# wait_for <what> <url>: poll until the URL answers 2xx.
wait_for() {
  local what=$1 url=$2 i
  for ((i = 0; i < 180; i++)); do
    if curl -fs -o /dev/null "$url"; then
      echo "$what is ready"
      return 0
    fi
    sleep 1
  done
  die "$what did not become ready at $url within 180s"
}

# mx <METHOD> <path> [json]: Matrix client API as @dev. The token stays in a header file so it never appears in argv.
mx() {
  local method=$1 path=$2
  [[ -s $DEV/matrix_auth_header ]] || die "no Matrix token; run 'testenv/dev.sh setup' first"
  local args=(-sS --fail-with-body -X "$method" -H "@$DEV/matrix_auth_header" "$MATRIX/_matrix/client/v3$path")
  if [[ $# -ge 3 ]]; then
    args+=(-H 'Content-Type: application/json' -d "$3")
  fi
  curl "${args[@]}"
}

# traq_password <user>: the admin keeps its dev password, other users get <name>-password.
traq_password() {
  if [[ $1 == traq ]]; then echo traq; else echo "$1-password"; fi
}

# traq_login <user> <cookie jar>: log in to traQ and store the session cookie.
traq_login_session() {
  local user=$1 jar=$2
  curl -sS --fail-with-body -o /dev/null -c "$jar" -X POST -H 'Content-Type: application/json' \
    -d "$(jq -n --arg n "$user" --arg p "$(traq_password "$user")" '{name: $n, password: $p}')" \
    "$TRAQ/api/v3/login"
}

# with_traq_session <user> <command...>: run a command with COOKIE_JAR set to a fresh logged-in session of the user.
# The cookie jar is removed on every exit path: the command runs in a subshell (so `die` cannot skip the cleanup), the
# status is captured, and INT/TERM remove the jar too.
with_traq_session() {
  local user=$1 status
  shift
  TRAQ_JAR=$(mktemp)
  trap 'rm -f "$TRAQ_JAR"; exit 130' INT TERM
  set +e
  (
    set -e
    traq_login_session "$user" "$TRAQ_JAR"
    COOKIE_JAR=$TRAQ_JAR "$@"
  )
  status=$?
  set -e
  trap - INT TERM
  rm -f "$TRAQ_JAR"
  return "$status"
}

build_tools() {
  mkdir -p "$DEV"
  (cd "$ROOT" && go build -tags goolm -o "$DEV/traq-matrix-bridge" ./cmd/traq-matrix-bridge \
    && go build -o "$DEV/cfgedit" ./testenv/cfgedit)
}

# generate_bridge_files: bridge config, appservice registration and the double puppet registration. Synapse reads the
# registrations at start, so this runs before the containers are started. network.oauth.client_id is filled in by setup.
generate_bridge_files() {
  cd "$DEV"
  rm -f config.yaml registration.yaml doublepuppet.yaml
  ./traq-matrix-bridge -c config.yaml -e >/dev/null
  ./cfgedit config.yaml \
    "network/server_url=$TRAQ" \
    "bridge/permissions={\"$SERVER_NAME\": admin}" \
    'bridge/mute_only_on_create=false' \
    'database/type=sqlite3-fk-wal' \
    "database/uri=file:$DEV/bridge.db?_txlock=immediate" \
    "homeserver/address=$MATRIX" \
    "homeserver/domain=$SERVER_NAME" \
    "appservice/address=http://host.docker.internal:$AS_PORT" \
    'appservice/hostname=0.0.0.0' \
    "appservice/port=$AS_PORT" \
    'encryption/allow=false' \
    'backfill/enabled=true' \
    'logging={min_level: debug, writers: [{type: stdout, format: pretty-colored}]}'
  # -g writes the generated as_token / hs_token back into config.yaml.
  ./traq-matrix-bridge -c config.yaml -r registration.yaml -g >/dev/null

  # Double puppeting with the appservice method: a second registration with no URL whose user namespace covers every
  # user on this homeserver (non-exclusive). The bridge authenticates as any user with its as_token.
  local as_token
  as_token=$(random_hex 32)
  cat >doublepuppet.yaml <<EOF
id: doublepuppet
url:
as_token: $as_token
hs_token: $(random_hex 32)
sender_localpart: doublepuppet-$(random_hex 8)
rate_limited: false
namespaces:
  users:
    - regex: '@.*:$SERVER_NAME'
      exclusive: false
EOF
  # The secret reaches cfgedit through the environment, not argv (which any local user can read from /proc).
  DOUBLE_PUPPET_SECRET="as_token:$as_token" ./cfgedit config.yaml 'double_puppet/servers={}' \
    "double_puppet/secrets={\"$SERVER_NAME\": \"\${DOUBLE_PUPPET_SECRET}\"}"
  chmod 600 config.yaml registration.yaml doublepuppet.yaml
}

# --- commands --------------------------------------------------------------

cmd_up() {
  build_tools
  [[ -f $DEV/registration.yaml ]] || generate_bridge_files
  "${COMPOSE[@]}" up -d
  wait_for traQ "$TRAQ/api/v3/version"
  wait_for Synapse "$MATRIX/_matrix/client/versions"
}

cmd_down() {
  "${COMPOSE[@]}" down -v --remove-orphans
  rm -rf "$DEV"
}

cmd_setup() {
  need_dev
  # traQ: OAuth client owned by the admin.
  if [[ ! -s $DEV/client_id ]]; then
    local client_id
    client_id=$(with_traq_session traq bash -c '
      curl -sS --fail-with-body -b "$COOKIE_JAR" -X POST -H "Content-Type: application/json" "$0/api/v3/clients" -d "$1"
    ' "$TRAQ" '{"name":"dev","description":"traq-matrix-bridge development","callbackUrl":"http://localhost:29399/callback","scopes":["read","write"],"confidential":false}' | jq -r .id)
    [[ -n $client_id && $client_id != null ]] || die "could not create the OAuth client"
    echo "$client_id" >"$DEV/client_id"
    "$DEV/cfgedit" "$DEV/config.yaml" "network/oauth/client_id=$client_id"
  fi
  echo "traQ OAuth client: $(cat "$DEV/client_id")"

  # traQ users. Signup is open, and the endpoint refuses requests that carry a session cookie, so no cookie here.
  local u status
  for u in alice bob; do
    status=$(curl -sS -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      -d "$(jq -n --arg n "$u" --arg p "$(traq_password "$u")" '{name: $n, password: $p}')" "$TRAQ/api/v3/users")
    case $status in
      201) echo "traQ user $u created (password $(traq_password "$u"))" ;;
      409) echo "traQ user $u exists" ;;
      *) die "creating traQ user $u: HTTP $status" ;;
    esac
  done

  # Matrix user @dev:localhost.
  if [[ ! -s $DEV/matrix_auth_header ]]; then
    "${COMPOSE[@]}" exec -T synapse register_new_matrix_user -u "$DEV_USER" -p "$DEV_PASSWORD" --no-admin \
      -c /config/homeserver.yaml http://localhost:8008 >/dev/null || echo "register_new_matrix_user failed (user may exist), trying to log in"
    local token
    token=$(curl -sS --fail-with-body -X POST -H 'Content-Type: application/json' \
      -d "$(jq -n --arg u "$DEV_USER" --arg p "$DEV_PASSWORD" '{type: "m.login.password", identifier: {type: "m.id.user", user: $u}, password: $p, initial_device_display_name: "dev.sh"}')" \
      "$MATRIX/_matrix/client/v3/login" | jq -r .access_token)
    [[ -n $token && $token != null ]] || die "could not log in as @$DEV_USER:$SERVER_NAME"
    (umask 077 && printf 'Authorization: Bearer %s\n' "$token" >"$DEV/matrix_auth_header")
  fi
  echo "Matrix user: @$DEV_USER:$SERVER_NAME (password $DEV_PASSWORD)"
}

cmd_bridge() {
  need_dev
  [[ -s $DEV/client_id ]] || die "run 'testenv/dev.sh setup' first"
  build_tools
  cd "$DEV"
  exec ./traq-matrix-bridge -c config.yaml -r registration.yaml
}

# management_room: the DM with the bridge bot; created (and the bot invited) when missing. An existing room must have
# the bot in it, otherwise commands would go unanswered.
management_room() {
  local file=$DEV/management_room room
  if [[ -s $file ]]; then
    room=$(<"$file")
    if mx GET /joined_rooms | jq -e --arg r "$room" '.joined_rooms | index($r)' >/dev/null; then
      mx GET "/rooms/$(urlencode "$room")/joined_members" | jq -e --arg b "$BOT_MXID" '.joined | has($b)' >/dev/null ||
        die "bot is not in $room; is the bridge running?"
      echo "$room"
      return 0
    fi
  fi
  room=$(mx POST /createRoom "$(jq -n --arg b "$BOT_MXID" '{invite: [$b], is_direct: true, preset: "trusted_private_chat", name: "traQ bridge management"}')" | jq -r .room_id)
  [[ -n $room && $room != null ]] || die "could not create the management room"
  echo "$room" >"$file"
  echo "$room"
}

cmd_send() {
  [[ $# -eq 2 ]] || die "usage: send <room> <text>"
  need_dev
  local txn
  txn="dev$(date +%s%N)$RANDOM"
  mx PUT "/rooms/$(urlencode "$1")/send/m.room.message/$txn" "$(jq -n --arg t "$2" '{msgtype: "m.text", body: $t}')" >/dev/null
}

cmd_bot() {
  [[ $# -ge 1 ]] || die "usage: bot <text>"
  need_dev
  local room
  room=$(management_room)
  cmd_send "$room" "$*"
}

cmd_tail() {
  need_dev
  local room=${1:-}
  [[ -n $room ]] || room=$(management_room)
  mx GET "/rooms/$(urlencode "$room")/messages?dir=b&limit=20" |
    jq -r '.chunk | reverse | .[] | "\(.sender): \(.content.body // ("[" + .type + "]"))"'
}

cmd_rooms() {
  need_dev
  local room name
  for room in $(mx GET /joined_rooms | jq -r '.joined_rooms[]'); do
    name=$(mx GET "/rooms/$(urlencode "$room")/state/m.room.name" 2>/dev/null | jq -r '.name // ""' || true)
    printf '%s\t%s\n' "$room" "$name"
  done
}

# traq_decide: approve the pending authorization of the session in COOKIE_JAR and print the redirect URL.
traq_decide() {
  local authorize_url=$1 headers
  headers=$(curl -sS -D - -o /dev/null -b "$COOKIE_JAR" -c "$COOKIE_JAR" "$authorize_url")
  grep -qi '^location: .*consent' <<<"$headers" || die "authorize URL did not lead to the consent page: $(head -1 <<<"$headers")"
  headers=$(curl -sS -D - -o /dev/null -b "$COOKIE_JAR" -c "$COOKIE_JAR" -X POST \
    --data-urlencode submit=approve "$TRAQ/api/v3/oauth2/authorize/decide")
  sed -n 's/^[Ll]ocation: *//p' <<<"$headers" | tr -d '\r'
}

cmd_traq_login() {
  [[ $# -eq 2 ]] || die "usage: traq-login <user> <authorize-url>"
  with_traq_session "$1" traq_decide "$2"
}

cmd_traq() {
  [[ $# -ge 3 ]] || die "usage: traq <user> <METHOD> <path> [json]"
  local user=$1 method=$2 path=$3
  shift 3
  with_traq_session "$user" bash -c '
    args=(-sS --fail-with-body -b "$COOKIE_JAR" -X "$1" "$0/api/v3$2")
    if [[ $# -ge 3 ]]; then args+=(-H "Content-Type: application/json" -d "$3"); fi
    curl "${args[@]}" && echo
  ' "$TRAQ" "$method" "$path" "$@"
}

main() {
  [[ $# -ge 1 ]] || { usage >&2; exit 2; }
  local cmd=$1
  shift
  case $cmd in
    up) cmd_up "$@" ;;
    down) cmd_down "$@" ;;
    setup) cmd_setup "$@" ;;
    bridge) cmd_bridge "$@" ;;
    bot) cmd_bot "$@" ;;
    tail) cmd_tail "$@" ;;
    rooms) cmd_rooms "$@" ;;
    send) cmd_send "$@" ;;
    traq-login) cmd_traq_login "$@" ;;
    traq) cmd_traq "$@" ;;
    -h | --help | help) usage ;;
    *) usage >&2; exit 2 ;;
  esac
}

main "$@"
