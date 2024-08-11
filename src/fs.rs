use std::ffi::OsString;
use std::iter::Skip;
use std::str::FromStr;
use std::time::SystemTime;

use fuse3::raw::prelude::*;
use fuse3::Result;
use futures_util::stream;
use futures_util::stream::Iter;
use std::ffi::OsStr;
use std::num::NonZeroU32;
use std::time::Duration;
use std::vec::IntoIter;

use crate::redis::RedisDriver;

const PARENT_INODE: u64 = 1;
const FILE_INODE: u64 = 2;
const PARENT_MODE: u16 = 0o755;
const FILE_MODE: u16 = 0o644;
const TTL: Duration = Duration::from_secs(1);
const STATFS: ReplyStatFs = ReplyStatFs {
    blocks: 1,
    bfree: 0,
    bavail: 0,
    files: 1,
    ffree: 0,
    bsize: 4096,
    namelen: u32::MAX,
    frsize: 0,
};

pub struct RedisFS {
    pub driver: RedisDriver,
}

macro_rules! or_enoent {
    ($expr:expr) => {
        match $expr {
            Ok(x) => x,
            Err(_) => return Err(libc::ENOENT.into()),
        }
    };
}

impl Filesystem for RedisFS {
    async fn init(&self, _req: Request) -> Result<ReplyInit> {
        Ok(ReplyInit {
            // 512mb
            // https://stackoverflow.com/questions/5606106/what-is-the-maximum-value-size-you-can-store-in-redis
            max_write: NonZeroU32::new(512 * 1024 * 1024).unwrap(),
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> Result<ReplyEntry> {
        if parent != PARENT_INODE {
            return Err(libc::ENOENT.into());
        }

        let id = or_enoent!(
            self.driver
                .open_key(name.to_str().unwrap_or_default())
                .await
        );

        Ok(ReplyEntry {
            ttl: TTL,
            attr: FileAttr {
                ino: id,
                size: 5000,
                blocks: 0,
                atime: SystemTime::now().into(),
                mtime: SystemTime::now().into(),
                ctime: SystemTime::now().into(),
                kind: FileType::RegularFile,
                perm: FILE_MODE,
                nlink: 0,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
            },
            generation: 0,
        })
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        if inode == PARENT_INODE {
            Ok(ReplyAttr {
                ttl: TTL,
                attr: FileAttr {
                    ino: PARENT_INODE,
                    size: 0,
                    blocks: 0,
                    atime: SystemTime::now().into(),
                    mtime: SystemTime::now().into(),
                    ctime: SystemTime::now().into(),
                    kind: FileType::Directory,
                    perm: PARENT_MODE,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 0,
                },
            })
        } else {
            Ok(ReplyAttr {
                ttl: TTL,
                attr: FileAttr {
                    ino: inode,
                    size: 5000,
                    blocks: 0,
                    atime: SystemTime::now().into(),
                    mtime: SystemTime::now().into(),
                    ctime: SystemTime::now().into(),
                    kind: FileType::RegularFile,
                    perm: FILE_MODE,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 0,
                },
            })
        }
    }

    async fn open(&self, _req: Request, inode: u64, flags: u32) -> Result<ReplyOpen> {
        if inode != PARENT_INODE {
            return Err(libc::ENOENT.into());
        }
        or_enoent!(self.driver.read(inode, 0, 1).await);

        Ok(ReplyOpen { fh: 0, flags })
    }

    async fn read(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        let data = or_enoent!(self.driver.read(inode, offset, size).await);
        Ok(ReplyData { data })
    }

    type DirEntryStream<'a> = Iter<Skip<IntoIter<Result<DirectoryEntry>>>> where Self: 'a;

    async fn readdir(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        _offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream<'_>>> {
        if inode != PARENT_INODE {
            return Err(libc::ENOENT.into());
        }

        let mut entries = vec![
            Ok(DirectoryEntry {
                inode: PARENT_INODE,
                kind: FileType::Directory,
                name: OsString::from("."),
                offset: 1,
            }),
            Ok(DirectoryEntry {
                inode: PARENT_INODE,
                kind: FileType::Directory,
                name: OsString::from(".."),
                offset: 2,
            }),
        ];

        let all = or_enoent!(self.driver.all_keys().await);
        let mut offset = 3;

        for (id, key) in all {
            entries.push(Ok(DirectoryEntry {
                inode: id,
                kind: FileType::RegularFile,
                name: or_enoent!(OsString::from_str(key.as_str())),
                offset,
            }));
            offset += 1;
        }

        Ok(ReplyDirectory {
            entries: stream::iter(entries.into_iter().skip(offset as usize)),
        })
    }

    async fn access(&self, _req: Request, inode: u64, _mask: u32) -> Result<()> {
        if inode != PARENT_INODE && inode != FILE_INODE {
            return Err(libc::ENOENT.into());
        }

        Ok(())
    }

    type DirEntryPlusStream<'a> = Iter<Skip<std::vec::IntoIter<Result<DirectoryEntryPlus>>>> where Self: 'a;

    async fn readdirplus(
        &self,
        _req: Request,
        parent: u64,
        _fh: u64,
        _offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'_>>> {
        if parent != PARENT_INODE {
            return Err(libc::ENOENT.into());
        }

        let mut entries = vec![
            Ok(DirectoryEntryPlus {
                inode: PARENT_INODE,
                generation: 0,
                kind: FileType::Directory,
                name: OsString::from("."),
                offset: 1,
                attr: FileAttr {
                    ino: PARENT_INODE,
                    size: 0,
                    blocks: 0,
                    atime: SystemTime::now().into(),
                    mtime: SystemTime::now().into(),
                    ctime: SystemTime::now().into(),
                    kind: FileType::Directory,
                    perm: PARENT_MODE,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 0,
                },
                entry_ttl: TTL,
                attr_ttl: TTL,
            }),
            Ok(DirectoryEntryPlus {
                inode: PARENT_INODE,
                generation: 0,
                kind: FileType::Directory,
                name: OsString::from(".."),
                offset: 2,
                attr: FileAttr {
                    ino: PARENT_INODE,
                    size: 0,
                    blocks: 0,
                    atime: SystemTime::now().into(),
                    mtime: SystemTime::now().into(),
                    ctime: SystemTime::now().into(),
                    kind: FileType::Directory,
                    perm: PARENT_MODE,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 0,
                },
                entry_ttl: TTL,
                attr_ttl: TTL,
            }),
        ];

        let all = or_enoent!(self.driver.all_keys().await);
        let mut offset = 3;

        for (id, key) in all {
            entries.push(Ok(DirectoryEntryPlus {
                inode: id,
                generation: 0,
                kind: FileType::Directory,
                name: or_enoent!(OsString::from_str(key.as_str())),
                offset: 3,
                attr: FileAttr {
                    ino: id,
                    size: 5000,
                    blocks: 0,
                    atime: SystemTime::now().into(),
                    mtime: SystemTime::now().into(),
                    ctime: SystemTime::now().into(),
                    kind: FileType::RegularFile,
                    perm: FILE_MODE,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 0,
                },
                entry_ttl: TTL,
                attr_ttl: TTL,
            }));
            offset += 1;
        }

        Ok(ReplyDirectoryPlus {
            entries: stream::iter(entries.into_iter().skip(offset as usize)),
        })
    }

    async fn statfs(&self, _req: Request, _inode: u64) -> Result<ReplyStatFs> {
        Ok(STATFS)
    }
}
