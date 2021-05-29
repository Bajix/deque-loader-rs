use crate::{key::Key, worker::TaskHandler};
use tokio::sync::oneshot;

pub trait Loadable<K: Key, T: TaskHandler<K>> {
  fn load_by<'a>(key: K) -> oneshot::Receiver<Result<Option<T::Value>, T::Error>>
  where
    K: Key,
    T: TaskHandler<K>;
}

#[macro_export]
macro_rules! define_static_loader {
  ($loader:ty, $key:ty) => {
    use $crate::{
      booter,
      loadable::Loadable,
      loader::{DataLoader, StaticLoaderExt},
      static_init,
    };

    #[static_init::dynamic(0)]
    static mut LOADER: DataLoader<$key, $loader> = DataLoader::new();

    impl StaticLoaderExt<$key, $loader> for DataLoader<$key, $loader> {
      fn loader() -> &'static DataLoader<$key, $loader> {
        unsafe { &LOADER }
      }
    }

    booter::call_on_boot!({
      <DataLoader<$key, $loader> as StaticLoaderExt<$key, $loader>>::loader()
        .start_detached_reactor();
    });
  };
  ($static_name:ident, $loader:ty, $key: ty) => {
    use $crate::{
      booter,
      loadable::Loadable,
      loader::{DataLoader, StaticLoaderExt},
      static_init,
    };

    #[static_init::dynamic(0)]
    static mut $static_name: DataLoader<$key, $loader> = DataLoader::new();

    impl StaticLoaderExt<$key, $loader> for DataLoader<$key, $loader> {
      fn loader() -> &'static DataLoader<$key, $loader> {
        unsafe { &$static_name }
      }
    }

    booter::call_on_boot!({
      <DataLoader<$key, $loader> as StaticLoaderExt<$key, $loader>>::loader()
        .start_detached_reactor();
    });
  };
}

#[macro_export]
macro_rules! attach_loader {
  ($loadable:ty, $loader:ty, $key:ty) => {
    impl $crate::loadable::Loadable<$key, $loader> for $loadable {
      fn load_by<'a>(
        key: $key,
      ) -> oneshot::Receiver<
        Result<
          Option<<$loader as TaskHandler<$key>>::Value>,
          <$loader as TaskHandler<$key>>::Error,
        >,
      > {
        use $crate::loader::{DataLoader, StaticLoaderExt};

        <DataLoader<$key, $loader> as StaticLoaderExt<$key, $loader>>::loader().load_by(key)
      }
    }
  };
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment};
  use std::{collections::HashMap, iter};

  #[derive(Default)]
  pub struct BatchLoader {}

  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct BatchSize(usize);

  #[async_trait::async_trait]
  impl TaskHandler<i32> for BatchLoader {
    type Value = BatchSize;
    type Error = ();

    async fn handle_task(
      task: Task<PendingAssignment<i32, BatchLoader>>,
    ) -> Task<CompletionReceipt<i32, BatchLoader>> {
      match task.get_assignment() {
        TaskAssignment::LoadBatch(task) => {
          let mut data: HashMap<i32, BatchSize> = HashMap::new();
          let keys = task.keys();

          println!("keys: {:?}", &keys);

          data.extend(
            task
              .keys()
              .into_iter()
              .zip(iter::repeat(BatchSize(keys.len()))),
          );

          task.resolve(Ok(data))
        }
        TaskAssignment::NoAssignment(receipt) => receipt,
      }
    }
  }

  define_static_loader!(BATCH_LOADER, BatchLoader, i32);
  attach_loader!(BatchSize, BatchLoader, i32);

  #[tokio::test]
  async fn it_loads() -> Result<(), ()> {
    booter::boot();

    let data = BatchSize::load_by(1_i32).await.unwrap()?;

    assert!(data.is_some());

    Ok(())
  }

  #[tokio::test]
  async fn it_auto_batches() -> Result<(), ()> {
    booter::boot();

    let a = BatchSize::load_by(2_i32);

    let _b = BatchSize::load_by(3_i32);

    let data = a.await.unwrap()?;

    if let Some(BatchSize(n)) = data {
      assert!(n.ge(&2));
    } else {
      panic!("Request failed to batch");
    }

    Ok(())
  }
}
