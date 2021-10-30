use super::client::{CLIENT, DATABASE_INDEX};
use arc_swap::{ArcSwap, Guard};
use futures_util::{future::FutureExt, StreamExt};
use redis::{
  aio::{ConnectionLike, MultiplexedConnection},
  Client, Cmd, ErrorKind, Pipeline, RedisError, RedisFuture, RedisResult, Value,
};
use std::sync::Arc;
use tokio::{sync::Notify, try_join};

struct InvalidationObserver {
  invalidate_cache_fn: Box<dyn Fn()>,
  invalidate_keys_fn: Box<dyn Fn(&[String])>,
}

inventory::collect!(InvalidationObserver);

impl InvalidationObserver {
  fn invalidate_cache(&self) {
    (self.invalidate_cache_fn)();
  }

  pub fn invalidate_keys(&self, keys: &[String]) {
    (self.invalidate_keys_fn)(keys);
  }
}

enum InvalidatorState {
  Idle,
  Connecting,
  ClientError(ErrorKind),
  ConnectionError(ErrorKind),
  Connected { client_id: i64 },
}
struct CacheInvalidator {
  state: ArcSwap<InvalidatorState>,
  notify: Notify,
}

impl Default for CacheInvalidator {
  fn default() -> Self {
    CacheInvalidator {
      state: ArcSwap::new(Arc::new(InvalidatorState::Idle)),
      notify: Notify::new(),
    }
  }
}

impl CacheInvalidator {
  async fn client_id(&'static self) -> RedisResult<i64> {
    let mut i = 0;

    loop {
      let current = self.state.load();

      match current.as_ref() {
        InvalidatorState::Idle => {
          self.establish_connection(current);
        }
        InvalidatorState::Connecting => {
          self.notify.notified().await;
        }
        InvalidatorState::ClientError(kind) => {
          break Err::<i64, _>(RedisError::from((
            kind.to_owned(),
            "Invalid Redis connection URL",
          )))
        }
        InvalidatorState::ConnectionError(kind) => {
          if kind.eq(&ErrorKind::IoError) && i.eq(&0) {
            self.establish_connection(current);
          } else {
            break Err::<i64, _>(RedisError::from((
              kind.to_owned(),
              "Unable to establish Redis connection",
            )));
          }
        }
        InvalidatorState::Connected { client_id } => break Ok(*client_id),
      }

      i = i + 1;
    }
  }

  fn store_and_notify(&self, state: InvalidatorState) {
    self.state.store(Arc::new(state));
    self.notify.notify_waiters();
  }

  fn establish_connection(&'static self, current: Guard<Arc<InvalidatorState>>) {
    let prev = self
      .state
      .compare_and_swap(&current, Arc::new(InvalidatorState::Connecting));

    if Arc::ptr_eq(&prev, &current) {
      match &*CLIENT {
        Ok(client) => {
          tokio::task::spawn(async move {
            if let Err(err) = self.start_new_connection(client).await {
              self.store_and_notify(InvalidatorState::ConnectionError(err.kind()));
            }
          });
        }
        Err(err) => self.store_and_notify(InvalidatorState::ClientError(err.kind())),
      }
    }
  }

  async fn start_new_connection(&'static self, client: &Client) -> RedisResult<()> {
    let mut conn = client.get_tokio_connection().await?;

    let client_id: i64 = redis::cmd("CLIENT")
      .arg("ID")
      .query_async(&mut conn)
      .await?;

    self
      .state
      .store(Arc::new(InvalidatorState::Connected { client_id }));

    for observer in inventory::iter::<InvalidationObserver> {
      observer.invalidate_cache();
    }

    self.notify.notify_waiters();

    tokio::task::spawn(async move {
      let mut pubsub = conn.into_pubsub();

      let result: RedisResult<()> = async {
        let _ = pubsub.subscribe("__redis__:invalidate").await?;

        let stream = pubsub.into_on_message();

        stream
          .map(|x| {
            // We can always know invalidation messages will be exactly so
            let keys: Vec<String> = x.get_payload().unwrap();

            keys
          })
          .for_each_concurrent(None, |keys| async move {
            for observer in inventory::iter::<InvalidationObserver> {
              observer.invalidate_keys(&keys);
            }
          })
          .await;

        Ok::<_, RedisError>(())
      }
      .await;

      match result {
        Ok(()) => self.store_and_notify(InvalidatorState::Idle),
        Err(err) => self.store_and_notify(InvalidatorState::ConnectionError(err.kind())),
      }
    });

    Ok(())
  }
}
enum ConnectionState {
  Idle,
  Connecting,
  ClientError(ErrorKind),
  ConnectionError(ErrorKind),
  Connected(MultiplexedConnection),
}

struct ConnectionManager {
  state: ArcSwap<ConnectionState>,
  invalidator: &'static CacheInvalidator,
  notify: Notify,
}

impl ConnectionManager {
  fn new(invalidator: &'static CacheInvalidator) -> Arc<ConnectionManager> {
    Arc::new(ConnectionManager {
      state: ArcSwap::from(Arc::new(ConnectionState::Idle)),
      invalidator,
      notify: Notify::new(),
    })
  }

  fn get_tracked_connection(self: &Arc<Self>) -> TrackedConnection {
    TrackedConnection(self.clone())
  }

  async fn get_multiplexed_connection(self: &Arc<Self>) -> RedisResult<Arc<ConnectionState>> {
    let mut i = 0;

    loop {
      let state = self.state.load_full();

      match state.as_ref() {
        ConnectionState::Idle => {
          self.clone().establish_connection(&state);
        }
        ConnectionState::Connecting => {
          self.notify.notified().await;
        }
        ConnectionState::ClientError(kind) => {
          break Err::<Arc<ConnectionState>, _>(RedisError::from((
            kind.to_owned(),
            "Invalid Redis connection URL",
          )))
        }
        ConnectionState::ConnectionError(kind) => {
          if kind.eq(&ErrorKind::IoError) && i.eq(&0) {
            self.clone().establish_connection(&state);
          } else {
            break Err::<Arc<ConnectionState>, _>(RedisError::from((
              kind.to_owned(),
              "Unable to establish Redis connection",
            )));
          }
        }
        ConnectionState::Connected(_) => break Ok(state),
      }

      i = i + 1;
    }
  }

  fn store_and_notify(self: &Arc<Self>, state: ConnectionState) {
    self.state.store(Arc::new(state));
    self.notify.notify_waiters();
  }

  fn establish_connection(self: Arc<Self>, current: &Arc<ConnectionState>) {
    let prev = self
      .state
      .compare_and_swap(current, Arc::new(ConnectionState::Connecting));

    if Arc::ptr_eq(&prev, current) {
      match &*CLIENT {
        Ok(client) => {
          tokio::task::spawn(async move {
            if let Err(err) = self.start_new_connection(client).await {
              self.store_and_notify(ConnectionState::ConnectionError(err.kind()))
            }
          });
        }
        Err(err) => self.store_and_notify(ConnectionState::ClientError(err.kind())),
      }
    }
  }

  async fn start_new_connection(self: &Arc<Self>, client: &Client) -> RedisResult<()> {
    let (mut conn, client_id) = try_join!(
      client.get_multiplexed_tokio_connection(),
      self.invalidator.client_id()
    )?;

    self.store_and_notify(ConnectionState::Connected(conn.clone()));

    self.set_tracking_redirect(client_id, &mut conn).await?;

    Ok(())
  }

  async fn set_tracking_redirect(
    &self,
    client_id: i64,
    conn: &mut MultiplexedConnection,
  ) -> RedisResult<()> {
    redis::cmd("CLIENT")
      .arg(&[
        "TRACKING",
        "ON",
        "REDIRECT",
        &client_id.to_string(),
        "BCAST",
        "NOLOOP",
      ])
      .query_async(conn)
      .await
  }
}

pub struct TrackedConnection(Arc<ConnectionManager>);

impl ConnectionLike for TrackedConnection {
  fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
    (async move {
      let state = self.0.get_multiplexed_connection().await?;

      let mut conn = match &*state {
        ConnectionState::Connected(conn) => conn.to_owned(),
        _ => unreachable!(),
      };

      match conn.req_packed_command(cmd).await {
        Ok(result) => Ok(result),
        Err(err) => {
          if err.is_connection_dropped() {
            self.0.clone().establish_connection(&state);
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
      let state = self.0.get_multiplexed_connection().await?;

      let mut conn = match &*state {
        ConnectionState::Connected(conn) => conn.to_owned(),
        _ => unreachable!(),
      };

      match conn.req_packed_commands(cmd, offset, count).await {
        Ok(result) => Ok(result),
        Err(err) => {
          if err.is_connection_dropped() {
            self.0.clone().establish_connection(&state);
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

/// An eventualistic [`MultiplexedConnection`] with Redis-assisted client-side cache invalidation tracking and managed reconnection
pub fn get_tracked_connection() -> TrackedConnection {
  #[static_init::dynamic(0)]
  static INVALIDATOR: CacheInvalidator = CacheInvalidator::default();

  thread_local! {
    static CONNECTION_MANAGER: Arc<ConnectionManager> = ConnectionManager::new(unsafe { &INVALIDATOR });
  }

  CONNECTION_MANAGER.with(|connection_manager| {
    connection_manager.get_tracked_connection()
  })
}
