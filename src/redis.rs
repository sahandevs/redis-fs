use std::sync::{atomic::AtomicU64, Arc};

use bytes::Bytes;
use log::warn;
use redis::AsyncCommands;

enum ValueState {
    Read(Vec<u8>),
    ToRead(Arc<String>),
}

pub struct RedisDriver {
    client: redis::Client,

    next_id: AtomicU64,

    id_to_value: dashmap::DashMap<u64, ValueState>,
}

impl RedisDriver {
    pub async fn new(conn_str: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(conn_str)?;
        Ok(Self {
            client,
            next_id: AtomicU64::new(5),
            id_to_value: Default::default(),
        })
    }

    pub async fn open_key(&self, key: &str) -> anyhow::Result<u64> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        self.id_to_value
            .insert(id, ValueState::ToRead(key.to_owned().into()));

        Ok(id)
    }

    pub async fn read(&self, id: u64, offset: u64, size: u32) -> anyhow::Result<Bytes> {
        let Some(val) = self.id_to_value.get(&id) else {
            anyhow::bail!("id not found")
        };
        match &*val {
            ValueState::Read(x) => {
                if x.len() < offset as _ {
                    anyhow::bail!("something went wrong");
                }
                let mut data = &x[offset as usize..];

                if data.len() > size as usize {
                    data = &data[..size as usize];
                }

                return Ok(Bytes::copy_from_slice(data));
            }
            ValueState::ToRead(key) => {
                warn!("{key}");
                let mut conn = self.client.get_multiplexed_async_connection().await?;
                let Some(val): Option<Vec<u8>> = conn.get(key).await? else {
                    anyhow::bail!("key not found!");
                };

                let mut data = &val.as_slice()[offset as usize..];
                warn!("{data:?}");
                if data.len() > size as usize {
                    data = &data[..size as usize];
                } else {
                    data = &data;
                }
                let data = Bytes::copy_from_slice(data);

                self.id_to_value.entry(id).insert(ValueState::Read(val));

                return Ok(data);
            }
        };
    }

    pub async fn close(&self, id: u64) -> anyhow::Result<()> {
        self.id_to_value.remove(&id);
        Ok(())
    }

    pub async fn all_keys(&self) -> anyhow::Result<Vec<(u64, Arc<String>)>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = conn.keys("*").await?;

        let mut id = self
            .next_id
            .fetch_add(keys.len() as _, std::sync::atomic::Ordering::Release);

        let mut out = vec![];
        for key in keys {
            let key_name = Arc::new(key);
            self.id_to_value
                .insert(id, ValueState::ToRead(key_name.clone()));
            out.push((id, key_name));
            id += 1;
        }

        Ok(out)
    }
}
