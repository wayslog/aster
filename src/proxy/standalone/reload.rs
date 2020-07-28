use futures::{Async, Future, Stream};
use hotwatch::{
    blocking::{Flow, Hotwatch},
    Event,
};
use log::Level;
use tokio::timer::Interval;

use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, Once};
use std::thread;
use std::time::{Duration, Instant};

use crate::com::*;
use crate::proxy::standalone::{Cluster, Request};

pub struct FileWatcher {
    watchfile: String,
    current: AtomicUsize,
    versions: Mutex<HashMap<usize, Config>>,
    reload: bool,
}

impl FileWatcher {
    fn new(watchfile: String, config: Config, reload: bool) -> Self {
        let init_version = 0;
        let mut init_map = HashMap::new();
        init_map.insert(init_version, config);
        FileWatcher {
            watchfile,
            current: AtomicUsize::new(init_version),
            versions: Mutex::new(init_map),
            reload,
        }
    }

    pub fn enable_reload(&self) -> bool {
        self.reload
    }

    pub fn get_config(&self, version: usize) -> Option<Config> {
        let handle = self.versions.lock().unwrap();
        handle.get(&version).cloned()
    }

    pub fn current_version(&self) -> Version {
        let current = self.current.load(Ordering::SeqCst);
        Version(current)
    }

    fn current_config(&self) -> Config {
        let current = self.current.load(Ordering::SeqCst);
        let handle = self.versions.lock().unwrap();
        handle
            .get(&current)
            .cloned()
            .expect("current version must be exists")
    }

    fn reload(&self) -> Result<(), AsError> {
        thread::sleep(Duration::from_millis(200));
        debug!("reload from file {}", &self.watchfile);
        let config = Config::load(&self.watchfile)?;
        config.valid()?;
        let current_config = self.current_config();

        if current_config.reload_equals(&config) {
            info!("skip due to no change in configuration");
            return Ok(());
        }

        info!("load new config content as {:?}", config);
        let current = self.current.load(Ordering::SeqCst);
        let mut handle = self.versions.lock().unwrap();
        handle.insert(current + 1, config);
        self.current.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn watch(&self) -> Result<(), AsError> {
        let delay = Duration::from_secs(3);
        let mut hwatch = Hotwatch::new_with_custom_delay(delay)
            .expect("file watcher must be initied by required reload");
        let watchfile = self.watchfile.clone();
        hwatch
            .watch(watchfile, |event: Event| {
                let fw = unsafe { G_FW.as_ref().unwrap() };
                match event {
                    Event::Write(path) | Event::Create(path) | Event::Rename(_, path) => {
                        info!("aware file changed for {:?}", path.as_os_str());
                        info!(
                            "start reload version from {}",
                            fw.current.load(Ordering::SeqCst)
                        );
                        if let Err(err) = fw.reload() {
                            error!("reload fail due to {:?}", err);
                        } else {
                            info!("success reload config");
                        }
                    }
                    _ => {}
                }
                Flow::Continue
            })
            .map_err(|err| {
                warn!("fail to watch file due to {:?}", err);
                AsError::BadConfig("reload".to_string())
            })?;
        Ok(hwatch.run())
    }
}

static G_FW_ONCE: Once = Once::new();
static mut G_FW: *const FileWatcher = std::ptr::null();

pub fn init(watchfile: &str, config: Config, reload: bool) -> Result<(), AsError> {
    G_FW_ONCE.call_once(|| {
        let fw = FileWatcher::new(watchfile.to_string(), config, reload);
        let fw = Box::new(fw);
        unsafe {
            G_FW = Box::into_raw(fw) as *const _;
        };
    });

    if reload {
        info!("starting file watcher");
        thread::spawn(move || {
            let fw = unsafe { G_FW.as_ref().unwrap() };
            if let Err(err) = fw.watch() {
                error!("fail to watch file due to {:?}", err);
            } else {
                info!("success start file watcher");
            }
        });
        thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

fn current_version() -> Version {
    let fw = unsafe { G_FW.as_ref().unwrap() };
    fw.current_version()
}

fn get_config(version: usize) -> Option<Config> {
    let fw = unsafe { G_FW.as_ref().unwrap() };
    fw.get_config(version)
}

fn enable_reload() -> bool {
    let fw = unsafe { G_FW.as_ref().unwrap() };
    fw.enable_reload()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Version(usize);

impl Version {
    fn config(&self) -> Option<Config> {
        get_config(self.0)
    }
}

pub struct Reloader<T> {
    name: String,
    cluster: Weak<Cluster<T>>,
    current: Version,
    interval: Interval,
    enable: bool,
}

impl<T: Request + 'static> Reloader<T> {
    pub fn new(cluster: Rc<Cluster<T>>) -> Self {
        let enable = enable_reload();
        let name = cluster.cc.borrow().name.clone();
        let weak = Rc::downgrade(&cluster);
        Reloader {
            name,
            enable,
            cluster: weak,
            current: Version(0),
            interval: Interval::new(
                Instant::now() + Duration::from_secs(10),
                Duration::from_secs(1),
            ),
        }
    }
}

impl<T> Future for Reloader<T>
where
    T: Request + 'static,
{
    type Item = ();
    type Error = ();
    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if !self.enable {
            debug!("success reload exists due reload not allow by cli arguments");
            return Ok(Async::Ready(()));
        }

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
            let current = current_version();
            if current == self.current {
                continue;
            }
            info!(
                "start change config version from {:?} to {:?}",
                self.current, current
            );
            if log_enabled!(Level::Debug) {
                let current_cfg = current.config();
                debug!("start to change config content as {:?}", current_cfg);
            }

            let config = match current.config() {
                Some(ccs) => ccs,
                None => {
                    debug!("fail to reload, config maybe uninited");
                    continue;
                }
            };

            let cc = match config.cluster(&self.name) {
                Some(cc) => cc,
                None => {
                    debug!("fail to reload, config absents cluster {}", self.name);
                    continue;
                }
            };
            if let Some(cluster) = self.cluster.upgrade() {
                if let Err(err) = cluster.reinit(cc) {
                    error!("fail to reload due to {:?}", err);
                    continue;
                }
                info!("success reload for cluster {}", cluster.cc.borrow().name);
                self.current = current;
            } else {
                error!("fail to reload due cluster has been destroyed");
            }
        }
    }
}
