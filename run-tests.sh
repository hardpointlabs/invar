#!/bin/bash

set -e

KV_BINARY="./invar"
PID_FILE="/tmp/kv-test-daemon.pid"
REDIS_LOG_FILE="/tmp/kv-test-redis.log"
MONGO_LOG_FILE="/tmp/kv-test-mongo.log"
REDIS_PORT=6379
MONGO_PORT=27017
MAX_WAIT=10

cleanup() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID" 2>/dev/null || true
            sleep 1
            kill -9 "$PID" 2>/dev/null || true
        fi
        rm -f "$PID_FILE"
    fi
    pkill -f "./invar" 2>/dev/null || true
}

# Stop whichever invar process is currently recorded in PID_FILE and wait
# for its port to be released before returning.
stop_daemon() {
    local port="$1"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID" 2>/dev/null || true
            # Wait up to MAX_WAIT seconds for the port to be released.
            for i in $(seq 1 $MAX_WAIT); do
                if ! (echo > /dev/tcp/localhost/$port) 2>/dev/null; then
                    break
                fi
                sleep 1
            done
            kill -9 "$PID" 2>/dev/null || true
        fi
        rm -f "$PID_FILE"
    fi
}

# Wait for a TCP port to start accepting connections.
wait_for_port() {
    local port="$1"
    local log="$2"
    echo "Waiting for daemon to be ready on port $port..."
    for i in $(seq 1 $MAX_WAIT); do
        if command -v nc >/dev/null 2>&1; then
            nc -z localhost $port 2>/dev/null && return 0
        fi
        if (echo > /dev/tcp/localhost/$port) 2>/dev/null; then
            return 0
        fi
        if [ $i -eq $MAX_WAIT ]; then
            echo "Daemon failed to start within ${MAX_WAIT}s"
            cat "$log"
            exit 1
        fi
        sleep 1
    done
}

trap cleanup EXIT

COMMIT=${1:-$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")}
VERSION=${2:-dev}

SHORT_COMMIT=${COMMIT:0:7}

echo "Building version $VERSION (commit $SHORT_COMMIT)..."
go build -ldflags="-X github.com/hardpointlabs/invar/config.Version=$VERSION -X github.com/hardpointlabs/invar/config.Commit=$SHORT_COMMIT" -o "$KV_BINARY" .

echo "Running unit & linearizability tests..."
go test ./...

# ---------------------------------------------------------------------------
# Phase 1 — Redis integration tests
# ---------------------------------------------------------------------------
echo "Starting invar daemon (redis) on port $REDIS_PORT..."
./invar redis > "$REDIS_LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

wait_for_port $REDIS_PORT "$REDIS_LOG_FILE"
echo "Redis daemon is ready."
echo ""

cd test
deno test --allow-net --allow-read main_test.ts pathological_test.ts

# ---------------------------------------------------------------------------
# Phase 2 — MongoDB wire-protocol integration tests
# ---------------------------------------------------------------------------
cd ..
stop_daemon $REDIS_PORT

echo ""
echo "Starting invar daemon (mongo) on port $MONGO_PORT..."
./invar mongo > "$MONGO_LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

wait_for_port $MONGO_PORT "$MONGO_LOG_FILE"
echo "Mongo daemon is ready."
echo ""

cd test
deno test --allow-net --allow-read mongo_wire_header_test.ts
