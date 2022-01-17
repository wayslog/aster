pub mod resp3;

use bytes::BytesMut;

pub trait Message {}

pub trait Codec {
    type Item: Message;
    type Error;

    fn deserialize(&mut self, src: &mut BytesMut) -> std::result::Result<Self::Item, Self::Error>;

    fn serialize(
        &mut self,
        item: &Self::Item,
        dst: &mut BytesMut,
    ) -> std::result::Result<usize, Self::Error>;
}
