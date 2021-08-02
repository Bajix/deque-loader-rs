use super::{error::DieselError, SimpleDieselError};
use crate::{
  key::Key,
  loader::{DataLoader, LocalLoader},
  task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment, TaskHandler},
};
use diesel_connection::{get_connection, PooledConnection};
use std::{collections::HashMap, sync::Arc};

/// a [`diesel`] specific loader interface using [`diesel_connection::get_connection`] for connection acquisition
pub trait DieselLoader: Sized + Send + Sync + 'static {
  type Key: Key;
  type Value: Send + Sync + Clone + 'static;
  const CORES_PER_WORKER_GROUP: usize = 4;
  fn load(
    conn: PooledConnection,
    keys: Vec<Self::Key>,
  ) -> Result<HashMap<Self::Key, Arc<Self::Value>>, DieselError>;
}
pub struct DieselHandler<T: DieselLoader>(T);

#[async_trait::async_trait]
impl<T> TaskHandler for DieselHandler<T>
where
  T: DieselLoader,
{
  type Key = T::Key;
  type Value = T::Value;
  type Error = SimpleDieselError;
  const CORES_PER_WORKER_GROUP: usize = T::CORES_PER_WORKER_GROUP;

  async fn handle_task(
    task: Task<PendingAssignment<Self::Key, Self::Value, Self::Error>>,
  ) -> Task<CompletionReceipt> {
    tokio::task::spawn_blocking(move || {
      let conn = get_connection();

      match task.get_assignment() {
        TaskAssignment::LoadBatch(task) => match conn {
          Ok(conn) => {
            let keys = task.keys();
            let result = T::load(conn, keys).map_err(|err| err.into());
            task.resolve(result)
          }
          Err(err) => task.resolve(Err(err.into())),
        },
        TaskAssignment::NoAssignment(receipt) => receipt,
      }
    })
    .await
    .unwrap()
  }
}

impl<Loader> LocalLoader for DieselHandler<Loader>
where
  Loader: DieselLoader + LocalLoader,
{
  type Handler = <Loader as LocalLoader>::Handler;
  fn loader() -> &'static std::thread::LocalKey<DataLoader<Self::Handler>> {
    Loader::loader()
  }
}
