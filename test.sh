#!/bin/bash
#fusermount -u ./target/test
mkdir -p ./target/test
cargo build
./target/debug/redis-fs mount --conn-str "redis://localhost/0" --mount-point ./target/test