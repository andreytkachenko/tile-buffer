mod cast;
mod range;
mod waker;

use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll, Waker},
};

use arrayvec::ArrayVec;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use waker::PollPendingQueue;

pub use range::AsyncRangeRead;

use crate::cast::CastExt;

pub const DEFAULT_TILE_SIZE: usize = 4096;
pub const MAX_TILE_COUNT: usize = 1 << TILE_COUNT_BITS;

const TILE_COUNT_BITS: usize = 5;
const TILE_COUNT_MASK: usize = MAX_TILE_COUNT - 1;

///
/// TileBuffer structure
///
pub struct TileBuffer<const N: usize, R: AsyncRangeRead + 'static> {
    ///
    /// Array of tiles. Size of this array usually shuld not be greated than 5
    tiles: [Tile<R>; N],

    ///
    /// Mapping between relative tile index (0..N) to global tile index (0..tile_total_count)
    /// example:
    ///   tiles: [{index: 12, data: Some}, {index: 13, data: Some}, {index: 14, data: Some}, {index: 15, data: None}]
    ///   tile_mapping: [12, 13, 14, 15]
    ///
    /// means:
    ///                                         |
    /// 00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16
    ///                                     == XX == ++      
    /// |  - offset pointer
    /// == - buffer loaded
    /// ++ - buffer is loading now
    /// XX - buffer loaded and current (relative index is 1; global index is 13)
    tile_mapping: [usize; N],

    ///
    /// Pointer of the current tile.
    /// example:
    ///   if tile_pointer is 0:
    ///     0 1 2 3 4 5 6 7 8
    ///         X = = =
    /// note: other tiles would load further data (good for sequential forward read)
    ///
    /// example:
    ///   if tile_pointer is 2:
    ///     0 1 2 3 4 5 6 7 8
    ///         = = X =
    /// note: most of other tiles would keep past data (good for random seeking within some distance from current offset)
    tile_pointer: usize,

    /// Size of one tile in bytes
    tile_size: usize,

    /// Effectivly total_size / tile_size
    tile_total_count: usize,

    /// Current offset in bytes
    offset: usize,

    /// Total size in bytes
    total_size: usize,

    /// Pending to poll next indexes with waker
    pending: Arc<PollPendingQueue>,
    inner: R,
}

impl<const N: usize, R: AsyncRangeRead> TileBuffer<N, R> {
    ///
    pub fn new(inner: R) -> Self {
        Self::new_with_tile_size_and_offset(inner, DEFAULT_TILE_SIZE, N / 2)
    }

    ///
    pub fn new_with_tile_size(inner: R, tile_size: usize) -> Self {
        Self::new_with_tile_size_and_offset(inner, tile_size, N / 2)
    }

    ///
    pub fn new_with_tile_size_and_offset(inner: R, tile_size: usize, tile_pointer: usize) -> Self {
        assert!(N <= 32, "Maximum number of tiles cannot be greater 32!");

        let total_size = inner.total_size();
        let tile_total_count = total_size / tile_size;

        let tile_total_count = if (total_size % tile_size) > 0 {
            tile_total_count + 1
        } else {
            tile_total_count
        };

        let pending = Arc::new(PollPendingQueue::default());

        Self {
            tiles: (0..N)
                .map(|i| {
                    let mut tile = Tile::new(i, tile_size, waker::create_waker(pending.clone(), i));
                    let offset = i * tile_size;
                    let length = usize::min(offset + tile_size, total_size) - offset;
                    tile.stage(&inner, offset, length);
                    tile
                })
                .cast(),
            tile_mapping: (0..N).cast(),
            tile_pointer,
            tile_size,
            tile_total_count,
            total_size,
            offset: 0,
            inner,
            pending,
        }
    }

    ///
    /// Set new offset
    ///   calling that method will recalculate mappings,
    ///   reuse already loaded tiles and stage the ones
    ///   which not loaded
    pub fn set_offset(&mut self, new_offset: usize) {
        self.offset = if new_offset > self.total_size {
            self.total_size
        } else {
            new_offset
        };

        let (tile_begin, _) = self.current_tile();
        let tile_end = tile_begin + N;

        let mut free: ArrayVec<usize, N> = ArrayVec::new();

        // clearing previous mappings
        self.tile_mapping.iter_mut().for_each(|x| *x = usize::MAX);

        // map existing ones
        for (idx, b) in self.tiles.iter().enumerate() {
            if b.index >= tile_begin && b.index < tile_end {
                self.tile_mapping[b.index - tile_begin] = idx;
            } else {
                free.push(idx);
            }
        }

        // map rest ones to free buffers and stage load appropriate ranges into those buffers
        for m in 0..N {
            if self.tile_mapping[m] == usize::MAX {
                let bindex = free.pop().unwrap();
                self.tiles[bindex].index = tile_begin + m;
                self.tile_mapping[m] = bindex;

                let offset = (tile_begin + m) * self.tile_size;
                let length = usize::min(offset + self.tile_size, self.total_size) - offset;

                self.tiles[bindex].stage(&self.inner, offset, length);
            }
        }
    }

    fn poll_tiles(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        self.pending.register_waker(cx.waker());

        loop {
            let Some(index) = self.pending.next_item() else {
                break Poll::Pending;
            };

            if let Poll::Ready(val) = self.tiles[index].poll(cx) {
                break Poll::Ready(match val {
                    Ok(_) => Ok(index),
                    Err(err) => Err(err),
                });
            }
        }
    }

    #[inline]
    fn current_tile(&self) -> (usize, usize) {
        let curr_tile = self.offset / self.tile_size;

        let tile_begin = if curr_tile < self.tile_pointer {
            0
        } else if curr_tile + self.tile_pointer < self.tile_total_count {
            curr_tile - self.tile_pointer
        } else if self.tile_total_count >= N {
            self.tile_total_count - N
        } else {
            0
        };

        (tile_begin, curr_tile - tile_begin)
    }

    fn current_buffer_read(&mut self, buf: &mut ReadBuf<'_>) -> Poll<usize> {
        let (begin, current) = self.current_tile();
        let current_tile_idx = begin + current;

        let mapped = self.tile_mapping[current];
        let tile = &mut self.tiles[mapped];

        if tile.task.is_some() {
            return Poll::Pending;
        }

        let begin_offset = current_tile_idx * self.tile_size;
        let tile_offset = self.offset - begin_offset;
        let tile_buffer_remaining = &tile.data[tile_offset..];

        let upto = usize::min(buf.remaining(), tile_buffer_remaining.len());

        buf.put_slice(&tile_buffer_remaining[..upto]);

        Poll::Ready(upto)
    }
}

impl<const N: usize, R: AsyncRangeRead> AsyncRead for TileBuffer<N, R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // TODO: use pin-project eventually
        let this = unsafe { self.get_unchecked_mut() };

        while let Poll::Ready(val) = this.poll_tiles(cx) {
            match val {
                Ok(_index) => (),
                Err(err) => return Poll::Ready(Err(err)),
            }
        }

        let remaining = buf.remaining();
        while let Poll::Ready(read) = this.current_buffer_read(buf) {
            this.set_offset(this.offset + read);

            if read == 0 || buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }
        }

        if remaining != buf.remaining() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl<const N: usize, R: AsyncRangeRead> AsyncSeek for TileBuffer<N, R> {
    fn start_seek(self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        let new_offset = match position {
            io::SeekFrom::Start(offset) => offset as _,
            io::SeekFrom::End(offset) => (self.total_size as i64 + offset).try_into().unwrap(),
            io::SeekFrom::Current(offset) => (self.offset as i64 + offset).try_into().unwrap(),
        };

        unsafe { self.get_unchecked_mut() }.set_offset(new_offset);

        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.offset as _))
    }
}

struct Tile<R: AsyncRangeRead + 'static> {
    index: usize,
    data: Vec<u8>,
    task: Option<R::Fut<'static>>,
    waker: Waker,
}

impl<R: AsyncRangeRead + 'static> Tile<R> {
    fn new(index: usize, tile_size: usize, waker: Waker) -> Self {
        Self {
            index,
            data: Vec::with_capacity(tile_size),
            task: None,
            waker,
        }
    }

    fn poll(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some(fut) = self.task.as_mut() {
            let mut ctx = Context::from_waker(&self.waker);

            // SAFTY:
            let pinned = unsafe { Pin::new_unchecked(fut) };
            ready!(pinned.poll(&mut ctx))?;

            drop(self.task.take());

            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn stage(&mut self, inner: &R, offset: usize, length: usize) {
        if self.data.len() != length {
            self.data.resize(length, 0);
        }

        let fut = inner.range_read(&mut self.data, offset);

        // SAFTY: Safe because it will live within 'self lifetime
        self.task = Some(unsafe { std::mem::transmute(fut) });
        self.waker.wake_by_ref();
    }
}

#[cfg(test)]
mod tests {
    use std::{io, pin::Pin};

    use super::{AsyncRangeRead, TileBuffer};
    use futures::Future;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    struct Test;
    impl AsyncRangeRead for Test {
        type Fut<'a> = Pin<Box<dyn Future<Output = io::Result<()>> + 'a>>
        where Self: 'a;

        fn total_size(&self) -> usize {
            50630
        }

        fn range_read<'a>(&'a self, buf: &'a mut [u8], offset: usize) -> Self::Fut<'a> {
            Box::pin(async move {
                let mut counter = offset;

                tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                for val in buf.iter_mut() {
                    *val = counter as _;
                    counter += 1;
                }

                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn sequential_read_test() {
        let inner = Test;

        let mut buff: TileBuffer<5, _> = TileBuffer::new_with_tile_size(inner, 1024);
        let mut data = Vec::new();

        let x = buff.read_to_end(&mut data).await.unwrap();

        let valid = (0..50630u32).map(|x| x as u8).collect::<Vec<u8>>();

        assert_eq!(x, 50630);
        assert_eq!(data, valid);
    }

    #[tokio::test]
    async fn random_read_test() {
        let inner = Test;

        let mut buff: TileBuffer<5, _> = TileBuffer::new_with_tile_size(inner, 1024);
        let mut data = [0u8; 256];
        let valid = (0u8..=255).collect::<Vec<u8>>();

        let x = buff.read_exact(&mut data).await.unwrap();
        assert_eq!(x, 256);
        assert_eq!(data.as_slice(), valid.as_slice());

        buff.seek(io::SeekFrom::Start(8448)).await.unwrap();

        let x = buff.read_exact(&mut data).await.unwrap();
        assert_eq!(x, 256);
        assert_eq!(data.as_slice(), valid.as_slice());

        buff.seek(io::SeekFrom::Start(7424)).await.unwrap();

        let x = buff.read_exact(&mut data).await.unwrap();
        assert_eq!(x, 256);
        assert_eq!(data.as_slice(), valid.as_slice());

        buff.seek(io::SeekFrom::Current(-1024)).await.unwrap();

        let x = buff.read_exact(&mut data).await.unwrap();
        assert_eq!(x, 256);
        assert_eq!(data.as_slice(), valid.as_slice());

        buff.seek(io::SeekFrom::Start(0)).await.unwrap();
        buff.seek(io::SeekFrom::End(-454)).await.unwrap();

        let x = buff.read_exact(&mut data).await.unwrap();
        assert_eq!(x, 256);
        assert_eq!(data.as_slice(), valid.as_slice());
    }
}
