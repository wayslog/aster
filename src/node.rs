use cmd::Cmd;
use com::*;
use resp::Resp;

use tokio::prelude::{Async, AsyncSink, Future, Sink, Stream};

use std::collections::VecDeque;

pub enum NodeConnState {
    Collect,
    Send,
    Wait,
    Return,
}

const MAX_NODE_CONN_CONCURRENCY: usize = 512;

pub struct NodeConn<S, O, NI, NO>
where
    S: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd>,
    NI: Stream<Item = Resp, Error = Error>,
    NO: Sink<SinkItem = Resp, SinkError = Error>,
{
    _node: String,
    cursor: usize,
    buffered: VecDeque<Cmd>,

    input: S,
    _resender: O,

    node_rx: NI,
    node_tx: NO,

    state: NodeConnState,
}

impl<S, O, NI, NO> NodeConn<S, O, NI, NO>
where
    S: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd>,
    NI: Stream<Item = Resp, Error = Error>,
    NO: Sink<SinkItem = Resp, SinkError = Error>,
{
    pub fn new(
        node: String,
        node_tx: NO,
        node_rx: NI,
        input: S,
        resender: O,
    ) -> NodeConn<S, O, NI, NO> {
        NodeConn {
            _node: node,
            cursor: 0,
            buffered: VecDeque::new(),
            input: input,
            _resender: resender,
            node_tx: node_tx,
            node_rx: node_rx,
            state: NodeConnState::Collect,
        }
    }
}

impl<S, O, NI, NO> Future for NodeConn<S, O, NI, NO>
where
    S: Stream<Item = Cmd, Error = Error>,
    O: Sink<SinkItem = Cmd>,
    NI: Stream<Item = Resp, Error = Error>,
    NO: Sink<SinkItem = Resp, SinkError = Error>,
{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            match self.state {
                NodeConnState::Collect => {
                    if self.buffered.len() == MAX_NODE_CONN_CONCURRENCY {
                        self.state = NodeConnState::Send;
                        continue;
                    }

                    match self.input.poll().map_err(|err| {
                        error!("fail to recv new command due err {:?}", err);
                    })? {
                        Async::Ready(Some(v)) => {
                            self.buffered.push_back(v);
                        }
                        Async::NotReady => {
                            if self.buffered.len() == 0 {
                                return Ok(Async::NotReady);
                            }
                            self.state = NodeConnState::Send;
                        }

                        Async::Ready(None) => {
                            self.cursor += 1;
                        }
                    }
                }

                NodeConnState::Send => {
                    if self.cursor == self.buffered.len() {
                        self.node_tx.poll_complete().map_err(|err| {
                            error!("fail to poll complete cmd resp {:?}", err);
                        })?;
                        self.state = NodeConnState::Wait;
                        self.cursor = 0;
                        continue;
                    }

                    let cursor = self.cursor;
                    let cmd = self.buffered.get(cursor).cloned().expect("cmd must exists");
                    trace!(
                        "trying to send into backend with cursor={} and buffered={}",
                        cursor,
                        self.buffered.len()
                    );
                    match self
                        .node_tx
                        .start_send(cmd.borrow().req.clone())
                        .map_err(|err| {
                            error!("fail to start send cmd resp {:?}", err);
                        })? {
                        AsyncSink::NotReady(_) => {
                            trace!("fail to send due to chan is full");
                        }
                        AsyncSink::Ready => {
                            self.cursor += 1;
                        }
                    };
                }
                NodeConnState::Wait => {
                    if self.cursor == self.buffered.len() {
                        self.cursor = 0;
                        self.state = NodeConnState::Return;
                        continue;
                    }

                    let cursor = self.cursor;
                    if let Some(resp) = try_ready!(self.node_rx.poll().map_err(|err| {
                        error!("fail to recv reply from node conn {:?}", err);
                    })) {
                        let mut cmd = self
                            .buffered
                            .get_mut(cursor)
                            .expect("resp mut exists")
                            .borrow_mut();
                        cmd.done(resp);
                        self.cursor += 1;
                    } else {
                        // TODO: set done with error
                        info!("quick done with error");
                    }
                }
                NodeConnState::Return => {
                    self.buffered.clear();
                    self.state = NodeConnState::Collect;
                }
            };
        }
    }
}
