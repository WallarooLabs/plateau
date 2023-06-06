#!/usr/bin/env bash
#
# Fetches data from a topic, iterating all records in all partitions and measuring elapsed time per request.

topic=${1}
set -u
order=${2:-'asc'}
page_size='1000'

echo "Starting iteration of ${topic}"
elapsed=$(\time -f 'real: %E ' curl -s -D out.err -o out.json "http://localhost:3030/topic/${topic}/records?order=${order}&page_size=${page_size}" \
    -d "{}" \
    -H "Content-Type: application/json" \
    -H "Accept: application/json; format=pandas-records" 2>&1)
status=$(cat out.err | grep iteration | cut -c21- | jq -r '.status' | tr -d ' ')
next=$(cat out.err | grep iteration | cut -c21- | jq -rc '.next')
next=${next:-'{}'}
count=$(cat out.json | jq length)
total=$count
status=${status:-'All'}
echo -e "\t$next - $status - $total (+$count) - $elapsed"
while [[ "$status" != 'All' ]]; do
    elapsed=$(\time -f 'real: %E' curl -s -D out.err -o out.json "http://localhost:3030/topic/${topic}/records?order=${order}&page_size=${page_size}" \
        -d "${next}" \
        -H "Content-Type: application/json" \
        -H "Accept: application/json; format=pandas-records" 2>&1)
    status=$(cat out.err | grep iteration | cut -c21- | jq -r '.status' | tr -d ' ')
    next=$(cat out.err | grep iteration | cut -c21- | jq -rc '.next')
    next=${next:-'{}'}
    count=$(cat out.json | jq length)
    total=$(expr $total + $count)
    status=${status:-'All'}
    echo -e "\t$next - $status - $total (+$count) - $elapsed"
done
echo "Final Status: $status / $total records"
