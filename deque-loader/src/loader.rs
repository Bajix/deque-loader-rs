use crate::{
  request::{ContextCache, Request},
  task::{CompletionReceipt, LoadBatch, Task, TaskHandler},
  Key,
};
use crossbeam::epoch;
use std::{
  cell::Cell,
  marker::PhantomData,
  mem, ptr,
  sync::{
    atomic::{AtomicIsize, Ordering},
    Arc,
  },
  thread::LocalKey,
};

// Minimum buffer capacity.
const MIN_CAP: usize = 64;
// Maximum number of tasks that can be stolen in `steal_batch()` and `steal_batch_and_pop()`.
const MAX_BATCH: usize = 32;
// If a buffer of at least this size is retired, thread-local garbage is flushed so that it gets
// deallocated as soon as possible.
const FLUSH_THRESHOLD_BYTES: usize = 1 << 10;

/// A buffer that holds tasks in a worker queue.
///
/// This is just a pointer to the buffer and its length - dropping an instance of this struct will
/// *not* deallocate the buffer.
struct Buffer<T> {
  /// Pointer to the allocated memory.
  ptr: *mut T,

  /// Capacity of the buffer. Always a power of two.
  cap: usize,
}

unsafe impl<T> Send for Buffer<T> {}

impl<T> Buffer<T> {
  /// Allocates a new buffer with the specified capacity.
  fn alloc(cap: usize) -> Buffer<T> {
    debug_assert_eq!(cap, cap.next_power_of_two());

    let mut v = Vec::with_capacity(cap);
    let ptr = v.as_mut_ptr();
    mem::forget(v);

    Buffer { ptr, cap }
  }

  /// Deallocates the buffer.
  unsafe fn dealloc(self) {
    drop(Vec::from_raw_parts(self.ptr, 0, self.cap));
  }

  /// Returns a pointer to the task at the specified `index`.
  unsafe fn at(&self, index: isize) -> *mut T {
    // `self.cap` is always a power of two.
    self.ptr.offset(index & (self.cap - 1) as isize)
  }

  /// Writes `task` into the specified `index`.
  ///
  /// This method might be concurrently called with another `read` at the same index, which is
  /// technically speaking a data race and therefore UB. We should use an atomic store here, but
  /// that would be more expensive and difficult to implement generically for all types `T`.
  /// Hence, as a hack, we use a volatile write instead.
  unsafe fn write(&self, index: isize, task: T) {
    ptr::write_volatile(self.at(index), task)
  }

  /// Reads a task from the specified `index`.
  ///
  /// This method might be concurrently called with another `write` at the same index, which is
  /// technically speaking a data race and therefore UB. We should use an atomic load here, but
  /// that would be more expensive and difficult to implement generically for all types `T`.
  /// Hence, as a hack, we use a volatile write instead.
  unsafe fn read(&self, index: isize) -> T {
    ptr::read_volatile(self.at(index))
  }
}

impl<T> Clone for Buffer<T> {
  fn clone(&self) -> Buffer<T> {
    Buffer {
      ptr: self.ptr,
      cap: self.cap,
    }
  }
}

impl<T> Copy for Buffer<T> {}

pub struct BatchQueue<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  slot: AtomicIsize,
  buffer: Cell<Buffer<Request<K, V, E>>>,
  /// Indicates that the worker cannot be shared among threads.
  _marker: PhantomData<*mut ()>, // !Send + !Sync
}

unsafe impl<K, V, E> Send for BatchQueue<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
}

impl<K, V, E> BatchQueue<K, V, E>
where
  K: Key,
  V: Send + Sync + Clone + 'static,
  E: Send + Sync + Clone + 'static,
{
  pub fn new() -> BatchQueue<K, V, E> {
    let buffer = Buffer::alloc(MIN_CAP);

    BatchQueue {
      slot: AtomicIsize::new(0),
      buffer: Cell::new(buffer),
      _marker: PhantomData,
    }
  }

  /// Resizes the internal buffer to the new capacity of `new_cap`.
  #[cold]
  unsafe fn resize(&self, new_cap: usize) {
    let slot = self.slot.load(Ordering::Relaxed);
    let buffer = self.buffer.get();

    // Allocate a new buffer and copy data from the old buffer to the new one.
    let new = Buffer::alloc(new_cap);
    for i in 0..slot {
      ptr::copy_nonoverlapping(buffer.at(i), new.at(i), 1);
    }

    let guard = &epoch::pin();

    // Replace the old buffer with the new one.
    let old = self.buffer.replace(new);

    self.slot.store(0, Ordering::Release);

    // Destroy the old buffer later.
    guard.defer_unchecked(move || old.dealloc());

    // If the buffer is very large, then flush the thread-local garbage in order to deallocate
    // it as soon as possible.
    if mem::size_of::<Request<K, V, E>>() * new_cap >= FLUSH_THRESHOLD_BYTES {
      guard.flush();
    }
  }

  pub fn push(&self, task: Request<K, V, E>) -> isize {
    let slot = self.slot.fetch_add(1, Ordering::Relaxed);
    let mut buffer = self.buffer.get();

    // Is the queue full?
    if slot >= buffer.cap as isize {
      // Yes. Grow the underlying buffer.
      unsafe {
        self.resize(2 * buffer.cap);
      }
      buffer = self.buffer.get();
    }

    // Write `task` into the slot.
    unsafe {
      buffer.write(slot, task);
    }

    slot
  }

  pub fn take_batch(&self) -> Vec<Request<K, V, E>> {
    let new = Buffer::alloc(MIN_CAP);
    let old = self.buffer.replace(new);

    let slot = self.slot.swap(0, Ordering::Release);

    unsafe { Vec::from_raw_parts(old.ptr, slot as usize, old.cap) }
  }
}

/// Each DataLoader is a thread local owner of a  [`crossbeam::deque::Worker`] deque for a given worker group
pub struct DataLoader<T: TaskHandler> {
  queue: BatchQueue<T::Key, T::Value, T::Error>,
}

impl<T> DataLoader<T>
where
  T: TaskHandler,
{
  pub fn new() -> Self {
    let queue = BatchQueue::new();

    DataLoader { queue }
  }

  fn push(loader: &'static LocalKey<DataLoader<T>>, req: Request<T::Key, T::Value, T::Error>) {
    if loader.with(|loader| loader.queue.push(req)).eq(&0) {
      tokio::task::spawn_local(async move {
        let requests = loader.with(|loader| loader.queue.take_batch());

        let task = Task::new(requests);

        T::handle_task(task).await;
      });
    }
  }

  pub async fn load_by(
    loader: &'static LocalKey<DataLoader<T>>,
    key: T::Key,
  ) -> Result<Option<Arc<T::Value>>, T::Error> {
    let (req, rx) = Request::new_oneshot(key);

    let local = tokio::task::LocalSet::new();

    local.spawn_local(async move {
      DataLoader::push(loader, req);
    });

    local.await;

    let data = rx.recv().await?;

    Ok(data)
  }

  pub async fn cached_load_by<RequestCache: Send + Sync + AsRef<ContextCache<T>>>(
    loader: &'static LocalKey<DataLoader<T>>,
    key: T::Key,
    request_cache: &RequestCache,
  ) -> Result<Option<Arc<T::Value>>, T::Error> {
    let (rx, req) = request_cache.as_ref().get_or_create(&key);

    if let Some(req) = req {
      let local = tokio::task::LocalSet::new();

      local.spawn_local(async move {
        DataLoader::push(loader, req);
      });

      local.await;
    }

    let data = rx.recv().await?;

    Ok(data)
  }

  pub fn delegate_assignment(
    loader: &'static LocalKey<DataLoader<T>>,
    task: Task<LoadBatch<T::Key, T::Value, T::Error>>,
  ) -> Task<CompletionReceipt> {
    let Task(LoadBatch { requests }) = task;

    for req in requests {
      DataLoader::push(loader, req);
    }

    Task::completion_receipt()
  }
}

pub trait StoreType {}
pub struct CacheStore;
impl StoreType for CacheStore {}
pub struct DataStore;
impl StoreType for DataStore {}
pub trait LocalLoader<Store: StoreType>: Sized + Send + Sync + 'static {
  type Handler: TaskHandler;
  fn loader() -> &'static LocalKey<DataLoader<Self::Handler>>;
}
