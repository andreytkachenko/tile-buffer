use std::io;

use futures::Future;

pub trait AsyncRangeRead {
    type Fut<'a>: Future<Output = io::Result<()>>
    where
        Self: 'a;

    /// Returns data total size
    fn total_size(&self) -> usize;

    /// Load `buf.len()` count of bytes from data starting from `offset` into `buf`
    fn range_read<'a>(&'a self, buf: &'a mut [u8], offset: usize) -> Self::Fut<'a>;
}
