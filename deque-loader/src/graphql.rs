use crate::{request::RequestCache, task::TaskHandler};
use async_graphql::Request;

#[doc(hidden)]
pub struct CacheFactory(Box<dyn Fn(Request) -> Request>);

impl CacheFactory {
  pub fn new<T>() -> Self
  where
    T: TaskHandler,
  {
    CacheFactory(Box::new(|request| request.data(RequestCache::<T>::new())))
  }

  pub fn insert_loader_cache(&self, request: Request) -> Request {
    (self.0)(request)
  }
}

inventory::collect!(CacheFactory);

/// Attach to a request a load cache instance for every registered type
pub fn insert_loader_caches(mut request: Request) -> Request {
  for factory in inventory::iter::<CacheFactory> {
    request = factory.insert_loader_cache(request);
  }

  request
}

/// Register cache factory for a [`TaskHandler`] using [`inventory`]. This will require adding [`inventory`] to your dependencies as well.
#[macro_export]
macro_rules! register_cache_factory {
  ($handler:ty) => {
    inventory::submit!({ $crate::graphql::CacheFactory::new::<$handler>() });

    impl AsRef<RequestCache<$handler>> for async_graphql::context::Context<'_> {
      fn as_ref(&self) -> &RequestCache<$handler> {
        self.data_unchecked()
      }
    }
  };
}
