use crate::{key::Key, loader::Loader, reactor::RequestReactor, request::Request};
use crossbeam::{
  atomic::AtomicCell,
  deque::{Steal, Stealer},
};
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

pub struct Task<T>(T);

pub struct PendingAssignment<K: Key, T: Loader<K>> {
  reactor_capacity: Arc<AtomicCell<i32>>,
  stealer: Stealer<Request<K, T>>,
}
pub struct LoadBatch<K: Key, T: Loader<K>> {
  requests: Vec<Request<K, T>>,
}
pub struct CompletionReceipt<K: Key, T: Loader<K>> {
  key: PhantomData<K>,
  loader: PhantomData<T>,
}
pub enum TaskAssignment<K: Key, T: Loader<K>> {
  LoadBatch(Task<LoadBatch<K, T>>),
  NoAssignment(Task<CompletionReceipt<K, T>>),
}

impl<K, T> Task<PendingAssignment<K, T>>
where
  K: Key,
  T: Loader<K>,
{
  #[must_use]
  pub(crate) fn fork_from_reactor(reactor: &RequestReactor<K, T>) -> Self {
    let reactor_capacity = reactor.scheduled_capacity.clone();
    let stealer = reactor.queue.stealer();

    reactor_capacity.fetch_add(T::MAX_BATCH_SIZE);

    Task(PendingAssignment {
      reactor_capacity,
      stealer,
    })
  }

  #[must_use]
  pub fn get_assignment(self) -> TaskAssignment<K, T> {
    let Task(PendingAssignment {
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

      TaskAssignment::LoadBatch(Task::from_requests(requests))
    } else {
      TaskAssignment::NoAssignment(Task::resolve_receipt())
    }
  }
}

impl<K, T> Task<LoadBatch<K, T>>
where
  K: Key,
  T: Loader<K>,
{
  fn from_requests(requests: Vec<Request<K, T>>) -> Self {
    Task(LoadBatch { requests })
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
  ) -> Task<CompletionReceipt<K, T>> {
    let Task(LoadBatch { requests }) = self;

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

    Task::<CompletionReceipt<K, T>>::resolve_receipt()
  }
}

impl<K, T> Task<CompletionReceipt<K, T>>
where
  K: Key,
  T: Loader<K>,
{
  fn resolve_receipt() -> Self {
    Task(CompletionReceipt {
      key: PhantomData,
      loader: PhantomData,
    })
  }
}