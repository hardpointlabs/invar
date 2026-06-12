#!/bin/bash

set -e

KV_BINARY="./invar"
PID_FILE="/tmp/kv-test-daemon.pid"
LOG_FILE="/tmp/kv-test-daemon.log"
PORT=6379
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

trap cleanup EXIT

echo "Building invar daemon..."
go build -o "$KV_BINARY" .

echo "Running unit & linearizability tests..."
go test ./...

echo "Starting invar daemon on port $PORT..."
./invar > "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

echo "Waiting for daemon to be ready..."
for i in $(seq 1 $MAX_WAIT); do
    if command -v nc >/dev/null 2>&1; then
        nc -z localhost $PORT 2>/dev/null && break
    fi
    if (echo > /dev/tcp/localhost/$PORT) 2>/dev/null; then
        break
    fi
    if [ $i -eq $MAX_WAIT ]; then
        echo "Daemon failed to start within ${MAX_WAIT}s"
        cat "$LOG_FILE"
        exit 1
    fi
    sleep 1
done

echo "Daemon is ready."
echo ""

cd test
deno test --allow-net --allow-read
