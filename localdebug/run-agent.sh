#!/bin/sh
set -e

mkdir -p cache/agent/
../target/statshouse -agent --cluster=test_shard_localhost --hostname=agent1 --agg-addr=127.0.0.1:13336,127.0.0.1:13336,127.0.0.1:13336 --cache-dir=/Users/e.martyn/Documents/vk/statshouse/localdebug/cache/agent "$@"
