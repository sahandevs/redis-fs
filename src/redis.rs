pub struct RedisDriver {
    conn: redis::aio::MultiplexedConnection,
    client: redis::Client,
}

impl RedisDriver {
    pub async fn new(conn_str: &str) -> anyhow::Result<Self> {
        let client = redis::Client::open(conn_str)?;
        let conn = client.get_multiplexed_async_connection().await?;

        Ok(Self { conn, client })
    }
}
