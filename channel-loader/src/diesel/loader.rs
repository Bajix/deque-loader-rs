use crate::{
  key::Key,
  task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment, TaskHandler},
};
use diesel_connection::{get_connection, PooledConnection};
use std::{collections::HashMap, marker::PhantomData};
use tokio::task::spawn_blocking;

use super::error::{DieselError, SimpleDieselError};

pub trait DieselWorker<K: Key>: Send + Sync {
  type Value: Send + Clone + 'static;
  const MAX_BATCH_SIZE: i32 = 100;
  fn load(conn: PooledConnection, keys: Vec<K>) -> Result<HashMap<K, Self::Value>, DieselError>;
}
pub struct DieselLoader<K: Key, T: DieselWorker<K>> {
  key: PhantomData<fn() -> K>,
  worker: PhantomData<fn() -> T>,
}

impl<K, T> Default for DieselLoader<K, T>
where
  K: Key,
  T: DieselWorker<K>,
{
  fn default() -> Self {
    DieselLoader {
      key: PhantomData,
      worker: PhantomData,
    }
  }
}

#[async_trait::async_trait]
impl<K, T> TaskHandler<K> for DieselLoader<K, T>
where
  K: Key,
  T: DieselWorker<K> + 'static,
{
  type Value = T::Value;
  type Error = SimpleDieselError;
  const MAX_BATCH_SIZE: i32 = T::MAX_BATCH_SIZE;

  async fn handle_task(task: Task<PendingAssignment<K, Self>>) -> Task<CompletionReceipt<K, Self>> {
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
