use crate::{request::LoadCache, task::TaskHandler};
use async_graphql::{context::Context, Request};

#[doc(hidden)]
pub struct CacheFactory(Box<dyn Fn(Request) -> Request>);

impl CacheFactory {
  pub fn new<T>() -> Self
  where
    T: TaskHandler,
  {
    CacheFactory(Box::new(|request| request.data(LoadCache::<T>::new())))
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
  };
}

impl<T> AsRef<LoadCache<T>> for Context<'_>
where
  T: TaskHandler,
{
  fn as_ref(&self) -> &LoadCache<T> {
    self.data_opt().unwrap_or_else(|| {
      panic!(
        "LoadCache<{}> hasn't been added to request data",
        tynm::type_name::<T>()
      )
    })
  }
}
