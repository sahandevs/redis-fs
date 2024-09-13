use std::sync::{atomic::AtomicU64, Arc};

use bytes::{BufMut, Bytes, BytesMut};
use redis::AsyncCommands;

enum ValueState {
    Read((Arc<String>, Vec<u8>)),
    ToRead(Arc<String>),
}

enum WriteMode {
    Overwrite,
    Append(u64),
}

struct WriteBuffer {
    mode: WriteMode,
    key: Arc<String>,
    buffer: BytesMut,
}

pub struct RedisDriver {
    client: redis::Client,

    next_id: AtomicU64,

    id_to_value: dashmap::DashMap<u64, ValueState>,

    write_buffers: dashmap::DashMap<u64, WriteBuffer>,
}

impl RedisDriver {
    pub async fn new(conn_str: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(conn_str)?;
        Ok(Self {
            client,
            next_id: AtomicU64::new(5),
            id_to_value: Default::default(),
            write_buffers: Default::default(),
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

    pub async fn write(&self, id: u64, offset: u64, bytes: &[u8]) -> anyhow::Result<()> {
        let mut write_buffer = {
            let entry = self.write_buffers.entry(id);
            let x = match entry {
                dashmap::Entry::Occupied(x) => x,
                dashmap::Entry::Vacant(x) => {
                    let Some(val) = self.id_to_value.get(&id) else {
                        anyhow::bail!("id not found")
                    };
                    let key = match &*val {
                        ValueState::Read((key, _)) | ValueState::ToRead(key) => key.clone(),
                    };
                    drop(val);
                    x.insert_entry(WriteBuffer {
                        mode: if offset == 0 {
                            WriteMode::Overwrite
                        } else {
                            WriteMode::Append(offset)
                        },
                        buffer: Default::default(),
                        key,
                    })
                }
            };

            x.into_ref()
        };
        // TODO: should I support out-of-order offsets?
        write_buffer.buffer.put(bytes);
        Ok(())
    }

    pub async fn flush_write_buffer(&self, id: u64) -> anyhow::Result<()> {
        let write_buffer = {
            let entry = self.write_buffers.entry(id);
            let x = match entry {
                dashmap::Entry::Occupied(x) => x,
                dashmap::Entry::Vacant(_) => anyhow::bail!("write buffer not found"),
            };
            x.remove()
        };

        let mut conn = self.client.get_multiplexed_async_connection().await?;

        match write_buffer.mode {
            WriteMode::Overwrite => {
                let val = &write_buffer.buffer[..];
                let _: () = conn.set(write_buffer.key.as_bytes(), val).await?;
            }
            WriteMode::Append(x) => {
                let mut val: Vec<u8> = conn.get(write_buffer.key.as_bytes()).await?;
                let _ = val.split_off(x as _);
                val.extend_from_slice(&write_buffer.buffer);
                let _: () = conn.set(write_buffer.key, val).await?;
            }
        }

        Ok(())
    }

    pub async fn read(&self, id: u64, offset: u64, size: u32) -> anyhow::Result<Bytes> {
        let Some(val) = self.id_to_value.get(&id) else {
            anyhow::bail!("id not found")
        };
        match &*val {
            ValueState::Read((_, x)) => {
                if x.len() < offset as _ {
                    anyhow::bail!("something went wrong");
                }
                let mut data = &x[offset as usize..];
                if data.len() > size as usize {
                    data = &data[..size as usize];
                } else {
                    data = &data;
                }
                let data = Bytes::copy_from_slice(data);
                return Ok(data);
            }
            ValueState::ToRead(key) => {
                let mut conn = self.client.get_multiplexed_async_connection().await?;
                let Some(value): Option<Vec<u8>> = conn.get(key).await? else {
                    anyhow::bail!("key not found!");
                };

                let mut data = &value.as_slice()[offset as usize..];
                if data.len() > size as usize {
                    data = &data[..size as usize];
                } else {
                    data = &data;
                }
                let data = Bytes::copy_from_slice(data);
                let key_clone = key.clone();
                drop(val);
                self.id_to_value
                    .entry(id)
                    .insert(ValueState::Read((key_clone, value)));

                return Ok(data);
            }
        };
    }

    pub async fn close(&self, id: u64) -> anyhow::Result<()> {
        self.id_to_value.remove(&id);
        let _ = self.flush_write_buffer(id).await;
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
