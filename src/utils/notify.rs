use futures::task::Task;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct Notify {
    task: Rc<RefCell<Option<Task>>>,
    expect: usize,
}

impl Notify {
    pub fn new(task: Task) -> Self {
        Notify {
            expect: 0,
            task: Rc::new(RefCell::new(Some(task))),
        }
    }

    pub fn empty() -> Self {
        Notify {
            expect: 0,
            task: Rc::new(RefCell::new(None)),
        }
    }

    pub fn set_expect(&mut self, num: usize) {
        self.expect = num;
    }

    pub fn set_task(&self, task: Task) {
        let mut mytask = self.task.borrow_mut();
        mytask.replace(task);
    }
}

impl Drop for Notify {
    fn drop(&mut self) {
        if Rc::strong_count(&self.task) == self.expect {
            if let Some(task) = self.task.borrow().as_ref() {
                task.notify();
            }
        }
    }
}
