use std::hash::Hash;
pub trait Key: Send + Hash + Ord + Eq + Clone + 'static {}
impl<T: Send + Hash + Ord + Eq + Clone + 'static> Key for T {}
