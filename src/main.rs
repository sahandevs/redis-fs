use clap::Parser;
use fuse3::MountOptions;
use log::error;
use redis::RedisDriver;

pub mod fs;
pub mod redis;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
enum Args {
    Mount {
        #[arg(short, long)]
        conn_str: String,

        #[arg(short, long)]
        mount_point: String,
    },
}

#[tokio::main]
async fn main() {
    if let Err(e) = start().await {
        error!("an error has occured: {e:?}");
    }
}

async fn start() -> anyhow::Result<()> {
    let args = Args::parse();
    env_logger::init();

    match args {
        Args::Mount {
            conn_str,
            mount_point,
        } => {
            let driver = RedisDriver::new(&conn_str).await?;
            let mut mount_options = MountOptions::default();
            let uid = unsafe { libc::getuid() };
            let gid = unsafe { libc::getgid() };
            mount_options
                .read_only(false)
                .fs_name("redis_fs")
                .uid(uid)
                .gid(gid);
            let mut session = fuse3::raw::Session::new(mount_options)
                .mount_with_unprivileged(fs::RedisFS { driver }, mount_point)
                .await?;
            let handle = &mut session;
            tokio::select! {
                res = handle => res.unwrap(),
                _ = tokio::signal::ctrl_c() => {
                    session.unmount().await.unwrap()
                }
            }
        }
    }

    Ok(())
}
