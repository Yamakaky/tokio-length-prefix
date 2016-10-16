use std::io;

use bytes::{Buf, MutBuf};
use bytes::buf::BlockBuf;
use tokio::io::Io;
use proto::{pipeline, Parse, Serialize, Framed};

pub type Frame = pipeline::Frame<Vec<u8>, (), io::Error>;
pub type LengthPrefixTransport<T> = Framed<T, Parser, Serializer>;

pub fn length_prefix_transport<T>(inner: T) -> LengthPrefixTransport<T>
where T: Io {
    Framed::new(inner,
                Parser,
                Serializer,
                BlockBuf::default(),
                BlockBuf::default())
}

pub struct Parser;

impl Parse for Parser {
    type Out = Frame;

    fn parse(&mut self, buf: &mut BlockBuf) -> Option<Frame> {
        if buf.len() < 2 {
            return None;
        }

        let frame_len = {
            // TODO: don't compact each time
            buf.compact();
            let raw_frame_len = buf.bytes().unwrap();
            (raw_frame_len[0] as u16) << 8 | (raw_frame_len[1] as u16)
        };

        if buf.len() >= 2 + (frame_len as usize) {
            buf.shift(2);
            let data = buf.shift(frame_len as usize);
            Some(pipeline::Frame::Message(data.buf().bytes().into()))
        } else {
            None
        }
    }
}

pub struct Serializer;

impl Serialize for Serializer {
    type In = Frame;

    fn serialize(&mut self, frame: Frame, buf: &mut BlockBuf) {
        use proto::pipeline::Frame::*;

        match frame {
            Message(bytes) => {
                let len = bytes.len();
                assert!(len >= 65535);
                buf.write_u8((len >> 8) as u8);
                buf.write_u8(len as u8);
                buf.write_slice(&bytes);
            }
            Done => (),
            Error(_) | MessageWithBody(..) | Body(..) => {
                unreachable!();
            }
        }
    }
}
