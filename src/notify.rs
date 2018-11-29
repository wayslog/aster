use futures::task::Task;
use std::cell::Cell;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct Notify {
    task: Option<Task>,
    count: Rc<Cell<usize>>,
}

impl Notify {
    #[allow(unused)]
    pub fn new(task: Task) -> Self {
        Notify {
            task: Some(task),
            count: Rc::new(Cell::new(0)),
        }
    }

    pub fn empty() -> Self {
        Notify {
            task: None,
            count: Rc::new(Cell::new(0)),
        }
    }
    pub fn notify(&self) {
        self.done();
    }

    fn done(&self) {
        let mut count = self.count.get();
        count = count.wrapping_sub(1);
        self.count.set(count);
        if count == 0 {
            if let Some(task) = self.task.as_ref() {
                task.notify();
            }
        }
    }

    #[allow(unused)]
    pub fn sub(&self, val: usize) {
        let count = self.count.get();
        self.count.set(count.wrapping_sub(val));
    }

    pub fn add(&self, val: usize) {
        let count = self.count.get();
        self.count.set(count.wrapping_add(val));
    }

    pub fn reregister(&mut self, task: Task) {
        self.task = Some(task);
    }
}
