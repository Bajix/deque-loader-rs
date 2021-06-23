use crate::{
  key::Key,
  loader::StaticLoaderExt,
  task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment, TaskHandler},
};
use diesel_connection::{get_connection, PooledConnection};
use std::{collections::HashMap, sync::Arc};
use tokio::task::spawn_blocking;

use super::error::{DieselError, SimpleDieselError};

/// a [`diesel`] specific loader interface using [`diesel_connection::get_connection`] for connection acquisition
pub trait DieselLoader: Send + Sync {
  type Key: Key;
  type Value: Send + Sync + Clone + 'static;
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
