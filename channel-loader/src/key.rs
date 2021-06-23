use std::hash::Hash;

/// Params to [`crate::Loadable::load_by`]; typically [`i32`] or newtype wrapper
pub trait Key: Send + Sync + Hash + Ord + Eq + Clone + 'static {}
impl<T: Send + Sync + Hash + Ord + Eq + Clone + 'static> Key for T {}
