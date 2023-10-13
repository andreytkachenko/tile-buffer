use std::mem::MaybeUninit;

pub(crate) trait CastExt<const N: usize, T>: Sized + Iterator<Item = T> {
    fn cast(mut self) -> [T; N] {
        let mut out: MaybeUninit<[T; N]> = MaybeUninit::uninit();

        (0..N).for_each(|i| {
            let item = self.next().expect("Array was not filled");

            unsafe {
                let slot = (out.as_mut_ptr() as *mut T).add(i);
                slot.write(item);
            };
        });

        unsafe { out.assume_init() }
    }
}
impl<const N: usize, T, V: Sized + Iterator<Item = T>> CastExt<N, T> for V {}
