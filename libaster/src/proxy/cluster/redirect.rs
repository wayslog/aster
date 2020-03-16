use futures::unsync::mpsc::Receiver;
use futures::{Async, AsyncSink, Future, Stream};

use crate::com::AsError;
use crate::proxy::cluster::fetcher;
use crate::proxy::cluster::{Cluster, Redirect, Redirection};

use std::rc::Rc;

pub struct RedirectHandler {
    cluster: Rc<Cluster>,
    moved_rx: Receiver<Redirection>,
    store: Option<Redirection>,
}

impl RedirectHandler {
    pub fn new(cluster: Rc<Cluster>, moved_rx: Receiver<Redirection>) -> RedirectHandler {
        RedirectHandler {
            cluster,
            moved_rx,
            store: None,
        }
    }
}

impl Future for RedirectHandler {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            if let Some(Redirection { target, cmd }) = self.store.take() {
                if !cmd.borrow().can_cycle() {
                    cmd.set_error(AsError::RequestReachMaxCycle);
                    continue;
                }

                let (slot, to, is_move) = match target {
                    Redirect::Move { slot, to } => (slot, to, true),
                    Redirect::Ask { slot, to } => (slot, to, false),
                };
                if is_move {
                    info!(
                        "cluster {} slot {} was moved to {}",
                        self.cluster.cc.borrow().name,
                        slot,
                        to
                    );
                    if self.cluster.update_slot(slot, to.clone()) {
                        self.cluster.trigger_fetch(fetcher::TriggerBy::Moved);
                    }
                }
                let rc_cmd = cmd.clone();
                match self.cluster.dispatch_to(&to, cmd) {
                    Ok(AsyncSink::NotReady(cmd)) => {
                        self.store = Some(Redirection::new(is_move, slot, to, cmd));
                        return Ok(Async::NotReady);
                    }
                    Ok(AsyncSink::Ready) => {
                        rc_cmd.borrow_mut().add_cycle();
                        std::mem::drop(rc_cmd);
                    }
                    Err(err) => {
                        error!("fail to dispath moved cmd to backend {} due to {}", to, err);
                    }
                }
            }

            match self.moved_rx.poll() {
                Ok(Async::Ready(Some(redirection))) => {
                    self.store = Some(redirection);
                }
                Ok(Async::Ready(None)) => {
                    info!("succeed to exits redirection handler");
                    return Ok(Async::Ready(()));
                }
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(err) => {
                    error!("fail to poll from moved channel due to {:?}", err);
                    return Err(());
                }
            }
        }
    }
}
