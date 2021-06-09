use crate::{key::Key, reactor::RequestReactor, request::Request};
use crossbeam::{
  atomic::AtomicCell,
  deque::{Steal, Stealer},
};
use itertools::Itertools;
use std::{
  collections::{HashMap, HashSet},
  marker::PhantomData,
  sync::Arc,
};

#[async_trait::async_trait]
pub trait TaskHandler: Default + Send + Sync {
  type Key: Key;
  type Value: Send + Clone + 'static;
  type Error: Send + Clone + 'static;
  const MAX_BATCH_SIZE: i32 = 100;
  async fn handle_task(task: Task<PendingAssignment<Self>>) -> Task<CompletionReceipt<Self>>;
}

pub struct Task<T>(T);
pub struct PendingAssignment<T: TaskHandler> {
  reactor_capacity: Arc<AtomicCell<i32>>,
  stealer: Stealer<Request<T>>,
}
pub struct LoadBatch<T: TaskHandler> {
  requests: Vec<Request<T>>,
}
pub struct CompletionReceipt<T: TaskHandler> {
  loader: PhantomData<fn() -> T>,
}
pub enum TaskAssignment<T: TaskHandler> {
  LoadBatch(Task<LoadBatch<T>>),
  NoAssignment(Task<CompletionReceipt<T>>),
}

impl<T> Task<PendingAssignment<T>>
where
  T: TaskHandler,
{
  #[must_use]
  pub(crate) fn fork_from_reactor(reactor: &RequestReactor<T>) -> Self {
    let reactor_capacity = reactor.scheduled_capacity.clone();
    let stealer = reactor.queue.stealer();

    reactor_capacity.fetch_add(T::MAX_BATCH_SIZE);

    Task(PendingAssignment {
      reactor_capacity,
      stealer,
    })
  }

  #[must_use]
  pub fn get_assignment(self) -> TaskAssignment<T> {
    let Task(PendingAssignment {
      reactor_capacity,
      stealer,
    }) = self;

    let mut keys: HashSet<T::Key> = HashSet::new();
    let mut requests: Vec<Request<T>> = Vec::with_capacity(T::MAX_BATCH_SIZE as usize);
    let mut received_count = 0;

    loop {
      if keys.len().ge(&(T::MAX_BATCH_SIZE as usize)) {
        break;
      }

      match stealer.steal() {
        Steal::Success(req) => {
          received_count += 1;

          if !req.tx.is_closed() {
            keys.insert(req.key.clone());
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
      requests.shrink_to_fit();

      TaskAssignment::LoadBatch(Task::from_requests(requests))
    } else {
      TaskAssignment::NoAssignment(Task::resolve_receipt())
    }
  }
}

impl<T> Task<LoadBatch<T>>
where
  T: TaskHandler,
{
  fn from_requests(requests: Vec<Request<T>>) -> Self {
    Task(LoadBatch { requests })
  }

  pub fn keys(&self) -> Vec<T::Key> {
    self
      .0
      .requests
      .iter()
      .map(|req| &req.key)
      .dedup()
      .map(|k| k.to_owned())
      .collect_vec()
  }

  #[must_use]
  pub fn resolve(
    self,
    results: Result<HashMap<T::Key, T::Value>, T::Error>,
  ) -> Task<CompletionReceipt<T>> {
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

    Task::<CompletionReceipt<T>>::resolve_receipt()
  }
}

impl<T> Task<CompletionReceipt<T>>
where
  T: TaskHandler,
{
  fn resolve_receipt() -> Self {
    Task(CompletionReceipt {
      loader: PhantomData,
    })
  }
}
