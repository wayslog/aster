use crate::com::AsError;
use crate::protocol::redis::{Cmd, Message};
use crate::proxy::cluster::{Ask, Cluster, Move, Redirect};

use futures::unsync::mpsc::SendError;
use futures::{Async, AsyncSink, Future, Sink, Stream};
use std::collections::VecDeque;
use std::rc::Rc;

const MAX_PIPELINE: usize = 64;

enum State {
    Running,
    Closing,
    Closed,
}

pub struct Back<I, O, R, M>
where
    I: Stream<Item = Cmd, Error = ()>,
    O: Sink<SinkItem = Cmd, SinkError = AsError>,
    R: Stream<Item = Message, Error = AsError>,
    M: Sink<SinkItem = Cmd, SinkError = SendError<Cmd>>,
{
    addr: String,
    state: State,

    store: Option<Cmd>,
    cmdq: VecDeque<Cmd>,
    recvq: VecDeque<Cmd>,

    input: I,
    output: O,
    recv: R,
    moved: M,
}

impl<I, O, R, M> Back<I, O, R, M>
where
    I: Stream<Item = Cmd, Error = ()>,
    O: Sink<SinkItem = Cmd, SinkError = AsError>,
    R: Stream<Item = Message, Error = AsError>,
    M: Sink<SinkItem = Cmd, SinkError = SendError<Cmd>>,
{
    fn try_forward(&mut self) -> Result<Async<()>, AsError> {
        let mut count = 0;
        loop {
            if let Some(cmd) = self.store.take() {
                let rcmd = cmd.clone();
                match self.output.start_send(cmd) {
                    Ok(AsyncSink::NotReady(cmd)) => {
                        self.store = Some(cmd);
                        break;
                    }
                    Ok(AsyncSink::Ready) => {
                        count += 1;
                        self.cmdq.push_back(rcmd);
                    }
                    Err(err) => {
                        error!(
                            "fail to send cmd to backend to {} due to {}",
                            self.addr, err
                        );
                        return Err(err);
                    }
                }
            }

            match self.input.poll() {
                Ok(Async::Ready(cmd)) => {
                    self.store = cmd;
                }
                Ok(Async::NotReady) => {
                    break;
                }
                Err(_) => unreachable!(),
            }

            if count != 0 && count % MAX_PIPELINE == 0 {
                self.output.poll_complete()?;
            }
        }

        if count > 0 && count % MAX_PIPELINE != 0 {
            self.output.poll_complete()?;
        }
        Ok(Async::NotReady)
    }

    fn try_recv(&mut self) -> Result<Async<()>, AsError> {
        loop {
            if self.cmdq.is_empty() {
                return Ok(Async::NotReady);
            }

            let is_ask = self
                .cmdq
                .front()
                .map(|x| x.borrow().is_ask())
                .expect("front always have cmd");

            // TODO: add redirect checker
            // match self.recv.poll() {
            //     Ok(Async::Ready(msg)) => {
            //         // check if it is redirect
            //         let redirect = msg.check_redirect();
            //     }
            //     Ok(Async::NotReady) => {
            //     }
            // }
        }
    }
}

impl<I, O, R, M> Future for Back<I, O, R, M>
where
    I: Stream<Item = Cmd, Error = ()>,
    O: Sink<SinkItem = Cmd, SinkError = AsError>,
    R: Stream<Item = Message, Error = AsError>,
    M: Sink<SinkItem = Cmd, SinkError = SendError<Cmd>>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        Ok(Async::NotReady)
    }
}
