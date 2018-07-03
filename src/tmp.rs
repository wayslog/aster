// fn handle(addr: &str) -> std::result::Result<(), ()> {
//     let (tx, rx) = channel(1024);
//     let cluster = Rc::new(RefCell::new(Cluster {
//         cc: ClusterConfig {
//             servers: Vec::new(),
//         },
//     }));

//     let pcluster = cluster.clone();
//     let dcluster = cluster.clone();

//     let dispatch = Dispatch {
//         cluster: dcluster,
//         rx: rx,
//         txs: vec![tx.clone()],
//     };

//     // TODO: add cluster to executor
//     let nodes: Vec<_> = (&*pcluster.borrow())
//         .cc
//         .servers
//         .clone()
//         .into_iter()
//         .map(|server| forword(server.to_owned()))
//         .collect();

//     let taddr = addr.parse().expect("bad listen address");
//     let listener = TcpListener::bind(&taddr).unwrap();
//     let server = listener
//         .incoming()
//         .map_err(|e| error!("accept fail: {}", e))
//         .for_each(move |sock| {
//             let handler = Handler {
//                 tx: tx.clone(),
//                 cluster: cluster.clone(),
//                 batch: MsgBatch::default(),
//                 sock: Socket {
//                     sock: sock,
//                     buf: BytesMut::new(),
//                     eof: false,
//                 },
//             };
//             tokio::executor::current_thread::spawn(handler);
//             Ok(())
//         });

//     let amt = join_all(nodes).join3(dispatch, server).map(|_| ());
//     tokio::executor::current_thread::block_on_all(amt)
// }

// fn forword(
//     addr: String,
// ) -> impl Future<Item = tokio::executor::Spawn, Error = ()> + 'static + Send {
//     let taddr = addr.parse().expect("bad server address");
//     TcpStream::connect(&taddr)
//         .map(move |sock| {
//             let node = Executor {
//                 addr: addr.to_owned(),
//                 sock: Socket {
//                     buf: BytesMut::new(),
//                     sock: sock,
//                     eof: false,
//                 },
//             };
//             tokio::executor::spawn(node)
//         })
//         .map_err(|err| error!("erro {:?}", err))
// }

// pub struct Socket {
//     buf: BytesMut,
//     sock: TcpStream,
//     eof: bool,
// }

// impl Socket {
//     fn buffer_mut(&mut self) -> &mut BytesMut {
//         &mut self.buf
//     }

//     fn buffer(&self) -> &BytesMut {
//         &self.buf
//     }

//     fn poll_read(&mut self) -> Poll<usize, IoError> {
//         loop {
//             if !self.buf.has_remaining_mut() {
//                 let len = self.buf.len();
//                 self.buf.reserve(len);
//             }

//             match self.sock.read_buf(&mut self.buf) {
//                 Ok(Async::Ready(size)) => {
//                     if size == 0 {
//                         self.eof = false;
//                     } else {
//                         return Ok(Async::Ready(size));
//                     }
//                 }
//                 other => {
//                     return other;
//                 }
//             };
//         }
//     }

//     fn poll_write(&mut self) -> Poll<usize, IoError> {
//         Ok(Async::NotReady)
//     }
// }

// pub struct Handler {
//     cluster: Rc<RefCell<Cluster>>,
//     sock: Socket,
//     tx: Sender<MsgBatch>,
//     batch: MsgBatch,
// }

// impl Future for Handler {
//     type Item = ();
//     type Error = ();

//     fn poll(&mut self) -> std::result::Result<Async<Self::Item>, Self::Error> {
//         loop {
//             match self.batch.decode(self.sock.buffer_mut()) {
//                 Ok(size) => {
//                     trace!("decoded msg: {}", size);
//                     self.sock.buf.advance(size);
//                 }
//                 Err(Error::MoreData) => {
//                     let mut batch = MsgBatch::default();
//                     {
//                         mem::swap(&mut self.batch, &mut batch);
//                     }
//                     // Call as stream
//                     self.tx.start_send(batch);
//                 }
//                 Err(err) => {
//                     error!("fail to parse {:?}", err);
//                 }
//             };
//             let _size = try_ready!(self.sock.poll_read().map_err(|err| {
//                 error!("error when proxy read: {:?}", err);
//             }));
//         }
//     }
// }

// pub struct ClusterConfig {
//     servers: Vec<String>,
// }

// pub struct Cluster {
//     cc: ClusterConfig,
// }

// unsafe impl Send for Cluster {}

// pub struct Dispatch {
//     cluster: Rc<RefCell<Cluster>>,
//     rx: Receiver<MsgBatch>,
//     txs: Vec<Sender<MsgBatch>>,
// }

// impl Future for Dispatch {
//     type Item = ();
//     type Error = ();

//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         Ok(Async::NotReady)
//     }
// }

// pub struct Executor {
//     addr: String,
//     sock: Socket,
// }

// impl Future for Executor {
//     type Item = ();
//     type Error = ();

//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         Ok(Async::NotReady)
//     }
// }
