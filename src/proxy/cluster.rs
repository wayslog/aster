use crate::com::AsError;
use crate::com::ClusterConfig;
use crate::protocol::redis::{Command, Message};

use failure::Error;
use futures::task::Task;
use futures::unsync::mpsc::{channel, Receiver, SendError, Sender};
use futures::AsyncSink;
use futures::{Sink, Stream};

use std::cell::RefCell;
use std::collections::HashMap;

pub struct Cluster {
    pub cc: ClusterConfig,

    slots: Slots,
}

struct Conns<C>(HashMap<String, C>);

struct Conn<S: Sink<SinkItem = Command>> {
    addr: String,
    primary: S,
    secondary: Option<S>,
}

enum State {
    Unchecked,
    Checked(u16),
}

pub struct Slots {
    state: State,
    masters: Vec<String>,
    replicas: Vec<Vec<String>>,
}
