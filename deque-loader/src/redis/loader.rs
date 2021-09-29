use super::{get_tracked_connection, TrackedConnection};
use crate::{
  key::Key,
  loader::{DataLoader, LocalLoader, StoreType},
  task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment, TaskHandler},
};
use redis::{ErrorKind, RedisError};
use std::{collections::HashMap, sync::Arc};

/// a [`redis`] specific loader interface using thread local multiplexed redis connections
#[async_trait::async_trait]
pub trait RedisLoader: Sized + Send + Sync + 'static {
  type Key: Key;
  type Value: Send + Sync + Clone + 'static;
  const CORES_PER_WORKER_GROUP: usize = 4;
  const MAX_BATCH_SIZE: Option<usize> = None;

  async fn load(
    conn: TrackedConnection,
    keys: Vec<Self::Key>,
  ) -> Result<HashMap<Self::Key, Arc<Self::Value>>, RedisError>;
}
pub struct RedisHandler<T: RedisLoader>(T);

#[async_trait::async_trait]
impl<T> TaskHandler for RedisHandler<T>
where
  T: RedisLoader,
{
  type Key = T::Key;
  type Value = T::Value;
  type Error = ErrorKind;

  async fn handle_task(
    task: Task<PendingAssignment<Self::Key, Self::Value, Self::Error>>,
  ) -> Task<CompletionReceipt> {
    match task.get_assignment::<Self>() {
      TaskAssignment::LoadBatch(task) => {
        let keys = task.keys();
        let conn = get_tracked_connection();
        let result = T::load(conn, keys).await.map_err(|err| err.kind());
        task.resolve(result)
      }
      TaskAssignment::NoAssignment(receipt) => receipt,
    }
  }
}

impl<Loader, Store> LocalLoader<Store> for RedisHandler<Loader>
where
  Loader: RedisLoader + LocalLoader<Store>,
  Store: StoreType,
{
  type Handler = <Loader as LocalLoader<Store>>::Handler;
  fn loader() -> &'static std::thread::LocalKey<DataLoader<Self::Handler>> {
    Loader::loader()
  }
}
