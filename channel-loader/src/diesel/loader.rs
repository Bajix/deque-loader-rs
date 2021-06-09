use crate::{
  key::Key,
  task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment, TaskHandler},
};
use diesel_connection::{get_connection, PooledConnection};
use std::{collections::HashMap, marker::PhantomData};
use tokio::task::spawn_blocking;

use super::error::{DieselError, SimpleDieselError};

pub trait DieselWorker: Send + Sync {
  type Key: Key;
  type Value: Send + Clone + 'static;
  const MAX_BATCH_SIZE: i32 = 100;
  fn load(
    conn: PooledConnection,
    keys: Vec<Self::Key>,
  ) -> Result<HashMap<Self::Key, Self::Value>, DieselError>;
}
pub struct DieselLoader<T: DieselWorker> {
  worker: PhantomData<fn() -> T>,
}

impl<T> Default for DieselLoader<T>
where
  T: DieselWorker,
{
  fn default() -> Self {
    DieselLoader {
      worker: PhantomData,
    }
  }
}

#[async_trait::async_trait]
impl<T> TaskHandler for DieselLoader<T>
where
  T: DieselWorker,
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
