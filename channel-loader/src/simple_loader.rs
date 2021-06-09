use crate::{
  key::Key,
  task::{CompletionReceipt, PendingAssignment, Task, TaskAssignment, TaskHandler},
};
use std::{collections::HashMap, marker::PhantomData};

/// a simplified loader interface with automatic task handling
#[async_trait::async_trait]
pub trait SimpleWorker: Send + Sync {
  type Key: Key;
  type Value: Send + Clone + 'static;
  type Error: Send + Clone + 'static;
  const MAX_BATCH_SIZE: i32 = 100;
  async fn load(keys: Vec<Self::Key>) -> Result<HashMap<Self::Key, Self::Value>, Self::Error>;
}

/// convenience [`TaskHandler`] wrapper implementation useful for whenever task assignment cannot be deferred.
pub struct SimpleLoader<T: SimpleWorker> {
  worker: PhantomData<fn() -> T>,
}

impl<T> Default for SimpleLoader<T>
where
  T: SimpleWorker,
{
  fn default() -> Self {
    SimpleLoader {
      worker: PhantomData,
    }
  }
}

#[async_trait::async_trait]
impl<T> TaskHandler for SimpleLoader<T>
where
  T: SimpleWorker,
{
  type Key = T::Key;
  type Value = T::Value;
  type Error = T::Error;
  const MAX_BATCH_SIZE: i32 = T::MAX_BATCH_SIZE;

  async fn handle_task(task: Task<PendingAssignment<Self>>) -> Task<CompletionReceipt<Self>> {
    match task.get_assignment() {
      TaskAssignment::LoadBatch(task) => {
        let keys = task.keys();
        let result = T::load(keys).await;
        task.resolve(result)
      }
      TaskAssignment::NoAssignment(receipt) => receipt,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{attach_loader, define_static_loader};
  use std::{collections::HashMap, iter};

  #[derive(Default)]
  pub struct FooLoader {}

  #[derive(Clone, Debug, PartialEq, Eq)]
  pub struct Foo;

  #[async_trait::async_trait]
  impl SimpleWorker for FooLoader {
    type Key = ();
    type Value = Foo;
    type Error = ();

    async fn load(keys: Vec<Self::Key>) -> Result<HashMap<Self::Key, Self::Value>, Self::Error> {
      let mut data: HashMap<(), Foo> = HashMap::new();

      data.extend(keys.into_iter().zip(iter::repeat(Foo)));

      Ok(data)
    }
  }

  define_static_loader!(SimpleLoader<FooLoader>);
  attach_loader!(Foo, SimpleLoader<FooLoader>);

  #[tokio::test]
  async fn it_loads() -> Result<(), ()> {
    booter::boot();

    let data = Foo::load_by(()).await.unwrap()?;

    assert!(data.is_some());

    Ok(())
  }
}
