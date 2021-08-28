use super::client::{CLIENT, DATABASE_INDEX};
use crossbeam::atomic::AtomicCell;
use futures_util::future::FutureExt;
use once_cell::sync::Lazy;
use redis::{
  aio::{ConnectionLike, MultiplexedConnection},
  Cmd, ErrorKind, Pipeline, RedisError, RedisFuture, RedisResult, Value,
};
use tokio::sync::watch::{self, Receiver, Sender};

#[derive(Clone)]
enum ConnectionState {
  Idle,
  Connecting,
  ClientError(ErrorKind),
  ConnectionError(ErrorKind),
  Connected(MultiplexedConnection),
}

/// A [`TrackedConnection`] is a proxy that wraps a [`watch::Receiver`] broadcast of lifecycle state corresponding to the current underlying singleton [`MultiplexedConnection`] and the succession of replacements thereafter. The current connection is always guaranteed to have Redis server-assisted client side caching enabled with redirection onto the current dedicated invalidation [`redis::aio::PubSub`] connection as for facilitating cluster-wide invalidation of client-side cache. This design allows us to fully utilize a lock-free, eventually consistent, concurrent LRU cache as a highly performant primary in-memory pre-cache while only falling back to Redis as necessary for ensuring eventual consistency and high cache availability. The trade off to be able to make it possible as to have such an LRU cache is that eviction is based off of presence within a fixed-size access log rather than cardinality. As such, irregardles of set size, anything not accessed within the last 1000 (Default) operations will eventually lack strong references and fallback to loading via the current singleton [`MultiplexedConnection`] or the underlying primary datastore loader.
#[derive(Clone)]
pub struct TrackedConnection {
  rx: Receiver<ConnectionState>,
}

impl TrackedConnection {
  async fn get_multiplexed_connection(&mut self) -> RedisResult<MultiplexedConnection> {
    loop {
      match &*self.rx.borrow() {
        ConnectionState::Idle => {
          establish_connection();
        }
        ConnectionState::Connecting => {}
        ConnectionState::ClientError(kind) => {
          break Err::<MultiplexedConnection, _>(RedisError::from((
            kind.to_owned(),
            "Invalid Redis connection URL",
          )))
        }
        ConnectionState::ConnectionError(kind) => {
          if kind.eq(&ErrorKind::IoError) {
            establish_connection();
          }
          break Err::<MultiplexedConnection, _>(RedisError::from((
            kind.to_owned(),
            "Unable to establish Redis connection",
          )));
        }
        ConnectionState::Connected(connection) => {
          if ESTABLISHING_CONNECTION.load().eq(&false) {
            break Ok::<_, RedisError>(connection.to_owned());
          }
        }
      };

      self.rx.changed().await.unwrap();
    }
  }
}

struct CacheConnectionManager {
  tx: Sender<ConnectionState>,
  rx: Receiver<ConnectionState>,
}

impl Default for CacheConnectionManager {
  fn default() -> Self {
    let (tx, rx) = watch::channel(ConnectionState::Idle);
    CacheConnectionManager { tx, rx }
  }
}

impl CacheConnectionManager {
  fn get_tracked_connection(&self) -> TrackedConnection {
    let rx = self.rx.clone();

    TrackedConnection { rx }
  }
}

static CONNECTION_MANAGER: Lazy<CacheConnectionManager> =
  Lazy::new(CacheConnectionManager::default);

/// An eventualistic [`MultiplexedConnection`] with Redis-assisted client-side cache invalidation tracking and managed reconnection
pub fn get_tracked_connection() -> TrackedConnection {
  CONNECTION_MANAGER.get_tracked_connection()
}

static ESTABLISHING_CONNECTION: AtomicCell<bool> = AtomicCell::new(false);
fn establish_connection() {
  if ESTABLISHING_CONNECTION
    .compare_exchange(false, true)
    .is_ok()
  {
    tokio::task::spawn(async {
      match &*CLIENT {
        Ok(client) => {
          let conn: RedisResult<MultiplexedConnection> = async {
            CONNECTION_MANAGER.tx.send(ConnectionState::Connecting).ok();

            let conn = client.get_multiplexed_tokio_connection().await?;

            Ok(conn)
          }
          .await;

          match conn {
            Ok(conn) => {
              CONNECTION_MANAGER
                .tx
                .send(ConnectionState::Connected(conn))
                .ok();
            }
            Err(err) => {
              CONNECTION_MANAGER
                .tx
                .send(ConnectionState::ConnectionError(err.kind()))
                .ok();
            }
          };
        }
        Err(err) => {
          CONNECTION_MANAGER
            .tx
            .send(ConnectionState::ClientError(err.kind()))
            .ok();
        }
      };

      ESTABLISHING_CONNECTION.store(false);
    });
  }
}

impl ConnectionLike for TrackedConnection {
  fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
    (async move {
      let mut conn = self.get_multiplexed_connection().await?;

      match conn.req_packed_command(cmd).await {
        Ok(result) => Ok(result),
        Err(err) => {
          if err.is_connection_dropped() {
            establish_connection();
          }

          Err(err)
        }
      }
    })
    .boxed()
  }

  fn req_packed_commands<'a>(
    &'a mut self,
    cmd: &'a Pipeline,
    offset: usize,
    count: usize,
  ) -> RedisFuture<'a, Vec<Value>> {
    (async move {
      let mut conn = self.get_multiplexed_connection().await?;

      match conn.req_packed_commands(cmd, offset, count).await {
        Ok(result) => Ok(result),
        Err(err) => {
          if err.is_connection_dropped() {
            establish_connection();
          }

          Err(err)
        }
      }
    })
    .boxed()
  }

  fn get_db(&self) -> i64 {
    DATABASE_INDEX.load()
  }
}
