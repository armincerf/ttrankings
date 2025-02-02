#!/bin/bash

# Exit on error
set -e

# Function to cleanup background processes on exit
cleanup() {
    echo "Cleaning up..."
    if [ ! -z "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
    fi
}
trap cleanup EXIT

# Build the project
echo "Building project..."
cargo build

# Set test configuration
export QUESTDB_HOST="127.0.0.1"
export QUESTDB_PORT="9009"
export RATE_LIMIT_RPS="10"

# Start a mock QuestDB server
echo "Starting mock QuestDB server..."
nc -l 9009 > /dev/null 2>&1 &
MOCK_DB_PID=$!

# Start the server in the background
echo "Starting server..."
RUST_LOG=debug cargo run -- events &
SERVER_PID=$!

# Wait for server to start
echo "Waiting for server to start..."
for i in {1..30}; do
    if curl -s http://localhost:3000/stats > /dev/null; then
        echo "Server started successfully"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Server failed to start after 30 seconds"
        exit 1
    fi
    sleep 1
done

# Test the index endpoint
echo "Testing index endpoint..."
INDEX_RESPONSE=$(curl -s http://localhost:3000/)
if [[ "$INDEX_RESPONSE" == *"Scraper Status"* ]]; then
    echo "✅ Index endpoint test passed"
else
    echo "❌ Index endpoint test failed"
    echo "Response: $INDEX_RESPONSE"
    exit 1
fi

# Test the stats endpoint
echo "Testing stats endpoint..."
STATS_RESPONSE=$(curl -s http://localhost:3000/stats)
if [[ "$STATS_RESPONSE" == *"requests_per_second"* ]] && [[ "$STATS_RESPONSE" == *"total_matches_found"* ]]; then
    echo "✅ Stats endpoint test passed"
else
    echo "❌ Stats endpoint test failed"
    echo "Response: $STATS_RESPONSE"
    exit 1
fi

# Test the shutdown endpoint
echo "Testing shutdown endpoint..."
SHUTDOWN_RESPONSE=$(curl -s -w "%{http_code}" http://localhost:3000/shutdown)
if [[ "$SHUTDOWN_RESPONSE" == "200" ]]; then
    echo "✅ Shutdown endpoint test passed"
else
    echo "❌ Shutdown endpoint test failed"
    echo "Response: $SHUTDOWN_RESPONSE"
    exit 1
fi

# Wait for server to shutdown
sleep 1

# Cleanup mock server
if [ ! -z "$MOCK_DB_PID" ]; then
    kill $MOCK_DB_PID 2>/dev/null || true
fi

echo "All tests passed! 🎉" 