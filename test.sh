#!/bin/bash
fusermount -u ./target/z
mkdir -p ./target/z
cargo build
export RUST_LOG="trace"
./target/debug/redis-fs mount --conn-str "redis://localhost/0" --mount-point ./target/z &

sleep 1
ls -l ./target/z
wait