use std::{io, pin::Pin};

use futures::Future;
use tile_buffer::{AsyncRangeRead, TileBuffer};
use tokio::io::AsyncReadExt;

struct Test;
impl AsyncRangeRead for Test {
    type Fut<'a> = Pin<Box<dyn Future<Output = io::Result<()>> + 'a>>
        where Self: 'a;

    fn total_size(&self) -> usize {
        50630
    }

    fn range_read<'a>(&'a self, buf: &'a mut [u8], offset: usize) -> Self::Fut<'a> {
        println!("range_read size {}; offset {}", buf.len(), offset);

        Box::pin(async move {
            let mut counter = offset;

            for val in buf.iter_mut() {
                *val = counter as _;
                counter += 1;
            }

            Ok(())
        })
    }
}

#[tokio::main]
async fn main() {
    let inner = Test;

    let mut buff: TileBuffer<5, _> = TileBuffer::new_with_tile_size(inner, 1024);
    let mut data = Vec::new();

    let x = buff.read_to_end(&mut data).await.unwrap();

    let valid = (0..50630u32).map(|x| x as u8).collect::<Vec<u8>>();

    assert_eq!(x, 50630);
    assert_eq!(data, valid);
}
