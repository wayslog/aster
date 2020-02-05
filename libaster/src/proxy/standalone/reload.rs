use futures::{Async, Future, Stream};
use tokio::timer::Interval;

use std::rc::Weak;

use crate::com::ClusterConfig;
use crate::proxy::standalone::{Cluster, Request};

pub struct FileWatcher {}

impl FileWatcher {
    pub fn get_current_version(&self) -> Version {
        Version(0)
    }
}

fn get_current_version() -> Version {
    Version(0)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version(usize);

impl Version {
    fn config(&self) -> ClusterConfig {
        unimplemented!()
    }
}

pub struct Reloader<T> {
    cluster: Weak<Cluster<T>>,
    current: Version,
    interval: Interval,
}

impl<T> Future for Reloader<T>
where
    T: Request + 'static,
{
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        loop {
            match self.interval.poll() {
                Ok(Async::Ready(_)) => {}
                Ok(Async::NotReady) => {
                    return Ok(Async::NotReady);
                }
                Err(err) => {
                    error!("fail to poll from timer {:?}", err);
                    return Err(());
                }
            }
            let current = get_current_version();
            if current == self.current {
                return Ok(Async::NotReady);
            }
            info!(
                "start change config version from {:?} to {:?}",
                self.current, current
            );

            let config = current.config();
            if let Some(cluster) = self.cluster.upgrade() {
                if let Err(err) = cluster.reinit(config) {
                    error!("fail to reload due to {:?}", err);
                    continue;
                }
                info!("success reload for cluster {}", cluster.cc.borrow().name);
            } else {
                return Ok(Async::Ready(()));
            }

            self.current = current;
        }
    }
}
