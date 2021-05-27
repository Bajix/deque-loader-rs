use crate::{
  key::Key,
  loader::{LoadTiming, Loader},
  request::Request,
  task::{PendingAssignment, Task},
};
use crossbeam::{atomic::AtomicCell, deque::Worker};
use flume::Receiver;
use std::sync::Arc;
use tokio::time::{timeout_at, Instant};

pub(crate) enum ReactorSignal<K: Key, T: Loader<K> + 'static> {
  Load(Request<K, T>, LoadTiming),
  EnterDeferralSpan,
  ExitDeferralSpan,
}
enum ReactorState {
  Idle,
  Collecting(i32),
  AwaitingDeadline(Instant),
  Draining,
  Yielding,
}

impl ReactorState {
  fn enter_deferral_span(&mut self) {
    *self = match self {
      ReactorState::Collecting(active_deferral_guards) => {
        ReactorState::Collecting(*active_deferral_guards + 1)
      }
      _ => ReactorState::Collecting(1),
    };
  }

  fn exit_deferral_span(&mut self) {
    *self = match self {
      ReactorState::Collecting(1) => ReactorState::Draining,
      ReactorState::Collecting(active_deferral_guards) => {
        ReactorState::Collecting(*active_deferral_guards - 1)
      }
      _ => unreachable!(),
    };
  }

  fn is_loading_deferred(&self) -> bool {
    matches!(self, ReactorState::Collecting(_))
  }
}

pub(crate) struct RequestReactor<K: Key, T: Loader<K> + 'static> {
  rx: Receiver<ReactorSignal<K, T>>,
  loader: Option<Arc<T>>,
  state: ReactorState,
  pub(crate) queue: Worker<Request<K, T>>,
  pub(crate) scheduled_capacity: Arc<AtomicCell<i32>>,
}

impl<K, T> RequestReactor<K, T>
where
  K: Key,
  T: Loader<K>,
{
  pub(crate) fn new(rx: Receiver<ReactorSignal<K, T>>) -> Self {
    RequestReactor {
      rx,
      loader: None,
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
    self.loader = Some(Arc::new(T::default()));

    while !self.rx.is_disconnected() {
      let signal = match &self.state {
        ReactorState::Idle => self.rx.recv_async().await.ok(),
        ReactorState::Collecting(_) => self.rx.recv_async().await.ok(),
        ReactorState::AwaitingDeadline(deadline) => {
          match timeout_at(deadline.to_owned(), self.rx.recv_async()).await {
            Ok(result) => result.ok(),
            Err(_) => {
              if !self.state.is_loading_deferred() {
                self.state = ReactorState::Draining;
              }
              None
            }
          }
        }
        ReactorState::Draining => self.rx.try_recv().ok(),
        ReactorState::Yielding => self.rx.try_recv().ok(),
      };

      match signal {
        Some(ReactorSignal::EnterDeferralSpan) => self.state.enter_deferral_span(),
        Some(ReactorSignal::ExitDeferralSpan) => self.state.exit_deferral_span(),
        Some(ReactorSignal::Load(request, timing)) => {
          self.queue.push(request);

          if self
            .scheduled_capacity
            .fetch_sub(1)
            .lt(&T::MAX_BATCH_SIZE.saturating_neg())
          {
            self.spawn_loader();
          } else {
            match (&self.state, &timing) {
              (&ReactorState::Idle, &LoadTiming::Immediate) => {
                self.state = ReactorState::Draining;
              }
              (&ReactorState::Idle, &LoadTiming::Deadline) => {
                self.state =
                  ReactorState::AwaitingDeadline(Instant::now().checked_add(T::MAX_DELAY).unwrap())
              }
              (&ReactorState::AwaitingDeadline(_), &LoadTiming::Immediate) => {
                self.state = ReactorState::Draining;
              }
              _ => (),
            }
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

    if let Some(loader) = &self.loader {
      let loader = loader.clone();

      tokio::task::spawn(async move {
        loader.handle_task(load_task).await;
      });
    }
  }
}
