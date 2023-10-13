use std::{
    sync::Arc,
    task::{RawWaker, RawWakerVTable, Waker},
};

use arrayvec::ArrayVec;
use futures::task::AtomicWaker;
use parking_lot::Mutex;

use crate::{MAX_TILE_COUNT, TILE_COUNT_BITS, TILE_COUNT_MASK};

#[derive(Default)]
#[repr(align(32))]
pub(crate) struct PollPendingQueue {
    waker: AtomicWaker,
    items: Mutex<ArrayVec<usize, MAX_TILE_COUNT>>,
}

impl PollPendingQueue {
    #[inline]
    fn enqueue(&self, index: usize) {
        self.items.lock().push(index);
        self.waker.wake();
    }

    #[inline]
    pub(crate) fn register_waker(&self, waker: &Waker) {
        self.waker.register(waker)
    }

    #[inline]
    pub(crate) fn next_item(&self) -> Option<usize> {
        self.items.lock().pop()
    }
}

pub(crate) fn create_waker<T>(arc: Arc<T>, index: usize) -> Waker {
    assert!(index < MAX_TILE_COUNT);
    assert!(std::mem::align_of::<T>() >= MAX_TILE_COUNT);

    let ptr = Arc::into_raw(arc) as usize;

    let raw_waker = RawWaker::new((ptr | index) as _, &TILE_BUFFER_WAKER_VTABLE);
    unsafe { Waker::from_raw(raw_waker) }
}

pub(crate) fn split_ptr_and_offset<T>(ptr_with_offset: *const ()) -> (Arc<T>, usize) {
    // clearing last TILE_COUNT_BITS bits
    let ptr = (ptr_with_offset as usize >> TILE_COUNT_BITS) << TILE_COUNT_BITS;

    (
        unsafe { Arc::from_raw(ptr as _) },
        // masking offset bits only
        ptr_with_offset as usize & TILE_COUNT_MASK,
    )
}

pub(crate) static TILE_BUFFER_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |ptr_with_offset| -> RawWaker {
        let (arc, offset) = split_ptr_and_offset::<PollPendingQueue>(ptr_with_offset);

        // cloning
        let clone = arc.clone();

        // forgetting original arc, to prevent drop
        std::mem::forget(arc);

        // making raw aligned (by 32) ptr of cloned Arc
        let clone_ptr = Arc::into_raw(clone);

        let res = clone_ptr as usize | offset;

        RawWaker::new(res as _, &TILE_BUFFER_WAKER_VTABLE)
    },
    |ptr_with_offset| {
        let (arc, offset) = split_ptr_and_offset::<PollPendingQueue>(ptr_with_offset);
        arc.enqueue(offset);
    },
    |ptr_with_offset| {
        let (arc, offset) = split_ptr_and_offset::<PollPendingQueue>(ptr_with_offset);
        arc.enqueue(offset);
        std::mem::forget(arc);
    },
    |ptr_with_offset| {
        let (arc, _) = split_ptr_and_offset::<PollPendingQueue>(ptr_with_offset);
        drop(arc)
    },
);
