use super::error::DieselError;
use crate::key::Key;
use diesel_connection::PooledConnection;
use std::{collections::HashMap, sync::Arc};

/// a [`diesel`] specific loader interface using [`diesel_connection::get_connection`] for connection acquisition
pub trait DieselLoader: Sized + Send + Sync {
  type Key: Key;
  type Value: Send + Sync + Clone + 'static;
  const CORES_PER_WORKER_GROUP: usize = 4;
  fn load(
    conn: PooledConnection,
    keys: Vec<Self::Key>,
  ) -> Result<HashMap<Self::Key, Arc<Self::Value>>, DieselError>;
}

/// Setup thread local [`DataLoader`] instances using a [`DieselLoader`] to define the [`TaskHandler`]
#[macro_export]
macro_rules! define_diesel_loader {
  ($loader:ty) => {
    #[$crate::async_trait::async_trait]
    impl $crate::task::TaskHandler for $loader {
      type Key = <$loader as $crate::diesel::DieselLoader>::Key;
      type Value = <$loader as $crate::diesel::DieselLoader>::Value;
      type Error = $crate::diesel::SimpleDieselError;
      const CORES_PER_WORKER_GROUP: usize = <$loader as DieselLoader>::CORES_PER_WORKER_GROUP;

      async fn handle_task(
        task: $crate::task::Task<$crate::task::PendingAssignment<Self>>,
      ) -> $crate::task::Task<$crate::task::CompletionReceipt<Self>> {
        tokio::task::spawn_blocking(move || {
          let conn = $crate::diesel_connection::get_connection();

          match task.get_assignment() {
            $crate::task::TaskAssignment::LoadBatch(task) => match conn {
              Ok(conn) => {
                let keys = task.keys();
                let result = <$loader>::load(conn, keys).map_err(|err| err.into());
                task.resolve(result)
              }
              Err(err) => task.resolve(Err(err.into())),
            },
            $crate::task::TaskAssignment::NoAssignment(receipt) => receipt,
          }
        })
        .await
        .unwrap()
      }
    }

    $crate::define_static_loader!($loader);
  };
}
