use clap::Parser;
use fuser::MountOption;
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
            let redis_driver = RedisDriver::new(&conn_str).await?;

            let options = vec![MountOption::RO, MountOption::FSName("redis-fs".to_string())];
            let mount_session = fuser::spawn_mount2(fs::HelloFS, mount_point, &options)?;

            tokio::signal::ctrl_c().await?;
            drop(mount_session);
        }
    }

    Ok(())
}
