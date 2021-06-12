use crate::{
  request::Request,
  task::{PendingAssignment, Task, TaskHandler},
};
use crossbeam::{atomic::AtomicCell, deque::Worker};
use flume::Receiver;
use log::trace;
use std::sync::Arc;

pub(crate) enum ReactorSignal<T: TaskHandler> {
  Load(Request<T>),
}

#[derive(Debug)]
enum ReactorState {
  Idle,
  Draining,
  Yielding,
}

/// capacity aware task-scheduler
pub(crate) struct RequestReactor<T: TaskHandler + 'static> {
  rx: Receiver<ReactorSignal<T>>,
  state: ReactorState,
  pub(crate) queue: Worker<Request<T>>,
  pub(crate) scheduled_capacity: Arc<AtomicCell<i32>>,
}

impl<T> RequestReactor<T>
where
  T: TaskHandler,
{
  pub(crate) fn new(rx: Receiver<ReactorSignal<T>>) -> Self {
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
    let mut collected_batch_size = 0;
    while !self.rx.is_disconnected() {
      let signal = match &self.state {
        ReactorState::Idle => self.rx.recv_async().await.ok(),
        ReactorState::Draining => self.rx.try_recv().ok(),
        ReactorState::Yielding => self.rx.try_recv().ok(),
      };

      match signal {
        Some(ReactorSignal::Load(request)) => {
          self.queue.push(request);
          collected_batch_size += 1;

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
          trace!(
            "{:?} collected {} while {:?}",
            core::any::type_name::<T>(),
            collected_batch_size,
            &self.state
          );
          collected_batch_size = 0;

          if matches!(&self.state, &ReactorState::Yielding) {
            self.state = ReactorState::Idle;
            if self.scheduled_capacity.load().is_negative() {
              self.spawn_loader();
            }
          } else {
            self.state = ReactorState::Yielding;

            // I believe it to be the case that this yield is unecessary and I have seen no actual evidence to suggest keeping this will improve batching behavior.
            // Please report back if you have a use case that results in any messages whatsoever being captured while yielding, otherwise this will be removed
            tokio::task::yield_now().await;
          }
        }
      }
    }
  }

  fn spawn_loader(&mut self) {
    let load_task: Task<PendingAssignment<T>> = Task::fork_from_reactor(&self);

    tokio::task::spawn(async move {
      T::handle_task(load_task).await;
    });
  }
}
