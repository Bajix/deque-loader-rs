use crate::key::Key;
use crate::loader::{LoadTiming, Loader};
use crate::request::{Request, ResolverExt};
use flume::Receiver;
use std::sync::Arc;
use tokio::time::timeout_at;
use tokio::time::Instant;

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
  loader: Arc<T>,
  state: ReactorState,
  requests: Vec<Request<K, T>>,
}

impl<K, T> RequestReactor<K, T>
where
  K: Key,
  T: Loader<K>,
{
  pub(crate) fn new(rx: Receiver<ReactorSignal<K, T>>, loader: T) -> Self {
    let loader = Arc::new(loader);
    let requests: Vec<Request<K, T>> = Vec::with_capacity(T::MAX_BATCH_SIZE);

    RequestReactor {
      rx,
      loader,
      state: ReactorState::Idle,
      requests,
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
      };

      match signal {
        Some(ReactorSignal::EnterDeferralSpan) => self.state.enter_deferral_span(),
        Some(ReactorSignal::ExitDeferralSpan) => self.state.exit_deferral_span(),
        Some(ReactorSignal::Load(request, timing)) => {
          self.requests.push(request);

          if self.requests.capacity().eq(&0_usize) {
            self.process_pending_requests();
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
          if !self.requests.is_empty() {
            self.process_pending_requests();
            self.state = ReactorState::Idle;
          }
        }
      }
    }
  }

  fn process_pending_requests(&mut self) {
    let mut requests: Vec<Request<K, T>> = Vec::with_capacity(T::MAX_BATCH_SIZE);
    std::mem::swap(&mut self.requests, &mut requests);
    requests.sort_unstable_by(|a, b| a.key.cmp(&b.key));
    let mut keys: Vec<K> = requests.iter().map(|req| req.key.to_owned()).collect();

    keys.dedup();

    let loader = self.loader.clone();

    tokio::task::spawn(async move {
      requests.resolve(loader.load(keys).await);
    });
  }
}
