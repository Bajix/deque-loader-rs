use crate::{key::Key, task::TaskHandler};
use tokio::sync::oneshot;
pub(crate) struct Request<K: Key, T: TaskHandler<K>> {
  pub(crate) key: K,
  pub(crate) tx: oneshot::Sender<Result<Option<T::Value>, T::Error>>,
}

impl<K: Key, T: TaskHandler<K>> Request<K, T> {
  pub(crate) fn new(
    key: K,
  ) -> (
    Request<K, T>,
    oneshot::Receiver<Result<Option<T::Value>, T::Error>>,
  ) {
    let (tx, rx) = oneshot::channel();

    let request = Request { key, tx };

    (request, rx)
  }

  pub(crate) fn resolve(self, value: Result<Option<T::Value>, T::Error>) {
    self.tx.send(value).ok();
  }
}
