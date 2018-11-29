use futures::task::Task;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone)]
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
    pub fn notify(&self) {
        self.done();
    }

    fn done(&self) {
        let pv = self.count.fetch_sub(1, Ordering::Relaxed);
        if pv == 1 {
            if let Some(task) = self.task.as_ref() {
                task.notify();
            }
        }
    }

    #[allow(unused)]
    pub fn sub(&self, val: usize) {
        self.count.fetch_sub(val, Ordering::Relaxed);
    }

    pub fn add(&self, val: usize) {
        self.count.fetch_add(val, Ordering::Relaxed);
    }

    pub fn reregister(&mut self, task: Task) {
        self.task = Some(task);
    }
}
