pub use async_graphql::{EmptyMutation, EmptySubscription};

mod mutation_root;
mod query_root;

pub use query_root::QueryRoot;

pub type Schema<QueryRoot, EmptyMutation, EmptySubscription> =
  async_graphql::Schema<QueryRoot, EmptyMutation, EmptySubscription>;
