use crate::{
  key::Key,
  request::Request,
  task::{PendingAssignment, Task, TaskHandler},
};
use crossbeam::{atomic::AtomicCell, deque::Worker};
use flume::Receiver;
use std::sync::Arc;

pub(crate) enum ReactorSignal<K: Key, T: TaskHandler<K> + 'static> {
  Load(Request<K, T>),
}
enum ReactorState {
  Idle,
  Draining,
  Yielding,
}

pub(crate) struct RequestReactor<K: Key, T: TaskHandler<K> + 'static> {
  rx: Receiver<ReactorSignal<K, T>>,
  state: ReactorState,
  pub(crate) queue: Worker<Request<K, T>>,
  pub(crate) scheduled_capacity: Arc<AtomicCell<i32>>,
}

impl<K, T> RequestReactor<K, T>
where
  K: Key,
  T: TaskHandler<K>,
{
  pub(crate) fn new(rx: Receiver<ReactorSignal<K, T>>) -> Self {
    RequestReactor {
      rx,
      state: ReactorState::Idle,
      queue: Worker::new_fifo(),
      scheduled_capacity: Arc::new(AtomicCell::new(0)),
    }
  }

  // Cooperatively process incoming requests until receiver disconnected
  pub(crate) fn start_detached(self) {
    tokio::task::spawn(async move {
      self.start_event_loop().await;
    });
  }

  async fn start_event_loop(mut self) {
    while !self.rx.is_disconnected() {
      let signal = match &self.state {
        ReactorState::Idle => self.rx.recv_async().await.ok(),
        ReactorState::Draining => self.rx.try_recv().ok(),
        ReactorState::Yielding => self.rx.try_recv().ok(),
      };

      match signal {
        Some(ReactorSignal::Load(request)) => {
          self.queue.push(request);

          if self
            .scheduled_capacity
            .fetch_sub(1)
            .lt(&T::MAX_BATCH_SIZE.saturating_neg())
          {
            self.spawn_loader();
          } else if matches!(&self.state, &ReactorState::Idle) {
            self.state = ReactorState::Draining;
          }
        }
        None => {
          if matches!(&self.state, &ReactorState::Yielding) {
            self.state = ReactorState::Idle;

            if self.scheduled_capacity.load().is_negative() {
              self.spawn_loader();
            }
          } else {
            self.state = ReactorState::Yielding;
            tokio::task::yield_now().await;
          }
        }
      }
    }
  }

  fn spawn_loader(&mut self) {
    let load_task: Task<PendingAssignment<K, T>> = Task::fork_from_reactor(&self);

    tokio::task::spawn(async move {
      T::handle_task(load_task).await;
    });
  }
}
