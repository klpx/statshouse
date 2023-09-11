#!/bin/sh
set -e

mkdir -p cache/aggregator/
../target/statshouse --aggregator --agg-addr=127.0.0.1:13336  --cluster=test_shard_localhost  --kh=127.0.0.1:8123 --metadata-addr "127.0.0.1:2442" --cache-dir=/Users/e.martyn/Documents/vk/statshouse/localdebug/cache/aggregator "$@"
               #command: aggregator --cluster=test_shard_aggregator --log-level=trace --agg-addr=':13336' --kh=kh:8123 --metadata-addr=metadata:2442 --auto-create --cache-dir=/var/lib/statshouse/cache/aggregator
