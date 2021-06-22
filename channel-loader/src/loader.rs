use crate::{
  request::Request,
  task::{Task, TaskHandler},
};
use crossbeam::{
  atomic::AtomicCell,
  deque::{Stealer, Worker},
};
use std::{
  sync::{Arc, RwLock, RwLockReadGuard},
  thread::LocalKey,
};
use tokio::sync::oneshot;

struct Stealers {}

/// Core load channel responsible for receiving incoming load_by requests to be enqueued via thread local [`crossbeam::deque::Worker`] queues
pub struct DataLoader<T: TaskHandler + 'static> {
  pub(crate) stealer_index: usize,
  pub(crate) load_capacity: &'static AtomicCell<i32>,
  pub(crate) queue: Worker<Request<T>>,
}

impl<T> DataLoader<T>
where
  T: TaskHandler,
{
  pub fn new(
    load_capacity: &'static AtomicCell<i32>,
    task_stealers: &'static RwLock<Vec<Stealer<Request<T>>>>,
  ) -> Self {
    let queue = Worker::new_fifo();
    let mut task_stealers = task_stealers.write().unwrap();
    let stealer_index = task_stealers.len();
    task_stealers.push(queue.stealer());

    Self {
      stealer_index,
      load_capacity,
      queue,
    }
  }

  pub fn load_by(&self, key: T::Key) -> oneshot::Receiver<Result<Option<Arc<T::Value>>, T::Error>>
  where
    T: TaskHandler,
  {
    let (req, rx) = Request::new(key);

    self.queue.push(req);

    if self.load_capacity.fetch_sub(1).eq(&0) {
      let task = Task::new(self.stealer_index);
      tokio::task::spawn(async move {
        T::handle_task(task).await;
      });
    }

    rx
  }
}

pub trait StaticLoaderExt {
  fn loader() -> &'static LocalKey<DataLoader<Self>>
  where
    Self: TaskHandler;
  fn load_capacity() -> &'static AtomicCell<i32>;
  fn task_stealers<'a>() -> RwLockReadGuard<'a, Vec<Stealer<Request<Self>>>>
  where
    Self: TaskHandler;
}
