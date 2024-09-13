#!/bin/bash
fusermount -u ./target/point
mkdir -p ./target/point
cargo build
export RUST_LOG="trace"
./target/debug/redis-fs mount --conn-str "redis://localhost/0" --mount-point ./target/point &

sleep 1
ls -l ./target/point
wait
