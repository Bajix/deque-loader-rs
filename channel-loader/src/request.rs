use crate::task::TaskHandler;
use tokio::sync::oneshot;
pub(crate) struct Request<T: TaskHandler> {
  pub(crate) key: T::Key,
  pub(crate) tx: oneshot::Sender<Result<Option<T::Value>, T::Error>>,
}

impl<T: TaskHandler> Request<T> {
  pub(crate) fn new(
    key: T::Key,
  ) -> (
    Request<T>,
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
