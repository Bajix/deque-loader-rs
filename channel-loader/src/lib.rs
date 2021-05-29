#![allow(dead_code)]

#[doc(hidden)]
pub extern crate atomic_take;
#[doc(hidden)]
pub extern crate booter;
#[doc(hidden)]
pub extern crate crossbeam;
#[doc(hidden)]
pub extern crate static_init;

#[cfg(feature = "diesel-loader")]
pub mod diesel;
mod key;
mod loadable;
mod loader;
mod reactor;
mod request;
mod simple_loader;
pub mod task;

pub use key::Key;
pub use loadable::Loadable;
pub use simple_loader::{SimpleLoader, SimpleWorker};
