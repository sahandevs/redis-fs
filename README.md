# Redis-fs

> 🔨 this project is under construction and is not ready yet!

Access your redis database from the file system!


## Installation

```
# Install FUSE. see https://github.com/cberner/fuser#dependencies
cargo install fuse-fs
```



## TODO

- [x] read a key
- [x] list keys
- [ ] write a key
- [ ] write automatic tests
- [ ] make it more efficient
  - [ ] proper redis connection pool
  - [ ] streamed/buffered read and writes
  - [ ] use SCAN instead of KEYS for readdirplus
- [ ] properly implement stats for keys
