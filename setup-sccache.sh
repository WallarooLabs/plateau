#!/usr/bin/env bash

if [ -n "$GCS_CREDS" ]
then
 export SCCACHE_GCS_KEY_PATH="$GCS_CREDS"
 export SCCACHE_GCS_BUCKET="wallaroo-sccache"
 export SCCACHE_GCS_RW_MODE=READ_WRITE
fi

export SCCACHE_LOG="info,sccache::cache=debug"
export RUST_LOG="sccache=info"
export RUSTC_WRAPPER=sccache

sccache --show-stats

