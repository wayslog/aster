use futures::task::Task;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct Notify {
    task: Option<Task>,
    count: Rc<AtomicUsize>,
}

impl Notify {
    #[allow(unused)]
    pub fn new(task: Task) -> Self {
        Notify {
            task: Some(task),
            count: Rc::new(AtomicUsize::new(0)),
        }
    }

    pub fn empty() -> Self {
        Notify {
            task: None,
            count: Rc::new(AtomicUsize::new(0)),
        }
    }

    pub fn done_without_notify(&self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn done(&self) {
        let pv = self.count.fetch_sub(1, Ordering::Relaxed);
        if pv == 1 {
            if let Some(task) = self.task.as_ref() {
                task.notify();
            }
        }
    }

    pub fn add(&self, val: usize) {
        self.count.fetch_add(val, Ordering::Relaxed);
    }

    pub fn reregister(&mut self, task: Task) {
        self.task = Some(task);
    }
}

impl Clone for Notify {
    fn clone(&self) -> Notify {
        self.count.fetch_add(1, Ordering::Relaxed);
        Notify {
            task: self.task.clone(),
            count: self.count.clone(),
        }
    }
}
