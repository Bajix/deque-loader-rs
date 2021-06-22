use crate::{
  key::Key,
  loader::StaticLoaderExt,
  task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment, TaskHandler},
};
use diesel_connection::{get_connection, PooledConnection};
use std::{collections::HashMap, sync::Arc};
use tokio::task::spawn_blocking;

use super::error::{DieselError, SimpleDieselError};

/// a [`diesel`] specific loader interface designed with that optimizes batching around connection acquisition using [`diesel_connection::get_connection`].
pub trait DieselLoader: Send + Sync {
  type Key: Key;
  type Value: Send + Clone + 'static;
  const MAX_BATCH_SIZE: i32 = 2000;
  fn load(
    conn: PooledConnection,
    keys: Vec<Self::Key>,
  ) -> Result<HashMap<Self::Key, Arc<Self::Value>>, DieselError>;
}

#[async_trait::async_trait]
impl<T> TaskHandler for T
where
  T: StaticLoaderExt + Default + DieselLoader + 'static,
{
  type Key = T::Key;
  type Value = T::Value;
  type Error = SimpleDieselError;
  const MAX_BATCH_SIZE: i32 = T::MAX_BATCH_SIZE;

  async fn handle_task(task: Task<PendingAssignment<Self>>) -> Task<CompletionReceipt<Self>> {
    spawn_blocking(move || {
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
