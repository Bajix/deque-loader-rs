use crate::{key::Key, loader::Loader, reactor::RequestReactor, request::Request};
use crossbeam::{
  atomic::AtomicCell,
  deque::{Steal, Stealer},
};
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

pub struct LoadTask<T>(T);

pub struct TaskStealer<K: Key, T: Loader<K>> {
  reactor_capacity: Arc<AtomicCell<i32>>,
  stealer: Stealer<Request<K, T>>,
}
pub struct TaskBatch<K: Key, T: Loader<K>> {
  requests: Vec<Request<K, T>>,
}
pub struct CompletionReceipt<K: Key, T: Loader<K>> {
  key: PhantomData<K>,
  loader: PhantomData<T>,
}
pub enum TaskAssignment<K: Key, T: Loader<K>> {
  LoadBatch(LoadTask<TaskBatch<K, T>>),
  NoAssignment(LoadTask<CompletionReceipt<K, T>>),
}

impl<K, T> LoadTask<TaskStealer<K, T>>
where
  K: Key,
  T: Loader<K>,
{
  #[must_use]
  pub(crate) fn fork_from_reactor(reactor: &RequestReactor<K, T>) -> Self {
    let reactor_capacity = reactor.scheduled_capacity.clone();
    let stealer = reactor.queue.stealer();

    reactor_capacity.fetch_add(T::MAX_BATCH_SIZE);

    LoadTask(TaskStealer {
      reactor_capacity,
      stealer,
    })
  }

  #[must_use]
  pub fn get_assignment(self) -> TaskAssignment<K, T> {
    let LoadTask(TaskStealer {
      reactor_capacity,
      stealer,
    }) = self;

    let mut requests: Vec<Request<K, T>> = Vec::with_capacity(T::MAX_BATCH_SIZE as usize);
    let mut received_count = 0;

    loop {
      if requests.capacity().eq(&0) {
        break;
      }

      match stealer.steal() {
        Steal::Success(req) => {
          received_count += 1;

          if !req.tx.is_closed() {
            requests.push(req);
          }
        }
        Steal::Retry => continue,
        Steal::Empty => break,
      };
    }

    reactor_capacity.fetch_sub(T::MAX_BATCH_SIZE - received_count);

    if requests.len().gt(&0) {
      requests.sort_unstable_by(|a, b| a.key.cmp(&b.key));

      TaskAssignment::LoadBatch(LoadTask::from_requests(requests))
    } else {
      TaskAssignment::NoAssignment(LoadTask::resolve_receipt())
    }
  }
}

impl<K, T> LoadTask<TaskBatch<K, T>>
where
  K: Key,
  T: Loader<K>,
{
  fn from_requests(requests: Vec<Request<K, T>>) -> Self {
    LoadTask(TaskBatch { requests })
  }

  pub fn keys(&self) -> Vec<K> {
    let mut keys: Vec<K> = self
      .0
      .requests
      .iter()
      .map(|req| req.key.to_owned())
      .collect();

    keys.dedup();

    keys
  }

  #[must_use]
  pub fn resolve(
    self,
    results: Result<HashMap<K, T::Value>, T::Error>,
  ) -> LoadTask<CompletionReceipt<K, T>> {
    let LoadTask(TaskBatch { requests }) = self;

    match results {
      Ok(mut values) => {
        let mut iter = requests
          .into_iter()
          .filter(|req| !req.tx.is_closed())
          .peekable();

        while let Some(req) = iter.next() {
          let can_take_value = {
            if let Some(next_req) = iter.peek() {
              !next_req.key.eq(&req.key)
            } else {
              true
            }
          };

          let value = match can_take_value {
            true => values.remove(&req.key),
            false => values.get(&req.key).map(|value| value.to_owned()),
          };

          req.resolve(Ok(value));
        }
      }
      Err(e) => {
        for req in requests {
          req.resolve(Err(e.clone()));
        }
      }
    };

    LoadTask::<CompletionReceipt<K, T>>::resolve_receipt()
  }
}

impl<K, T> LoadTask<CompletionReceipt<K, T>>
where
  K: Key,
  T: Loader<K>,
{
  fn resolve_receipt() -> Self {
    LoadTask(CompletionReceipt {
      key: PhantomData,
      loader: PhantomData,
    })
  }
}
