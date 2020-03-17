use futures::task::Task;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct Notify {
    shared: NotifyShared,
}

impl Notify {
    pub fn empty() -> Self {
        Notify {
            shared: NotifyShared {
                task: Rc::new(RefCell::new(None)),
                count: Rc::new(Cell::new(1u16)),
                expect: std::u16::MAX,
            },
        }
    }

    pub fn set_task(&mut self, task: Task) {
        self.shared.task.borrow_mut().replace(task);
    }

    pub fn notify(&self) {
        if let Some(task) = self.shared.task.borrow().as_ref() {
            // trace!("trace notify Some");
            task.notify();
        } else {
            // trace!("trace notify None");
        }
    }

    pub fn set_expect(&mut self, expect: u16) {
        self.shared.expect = expect;
    }

    pub fn expect(&self) -> u16 {
        self.shared.expect
    }

    pub fn fetch_sub(&self, val: u16) -> u16 {
        let origin_val = self.shared.count.get();
        self.shared.count.set(origin_val - val);
        origin_val
    }

    pub fn fetch_add(&self, val: u16) -> u16 {
        let origin_val = self.shared.count.get();
        self.shared.count.set(origin_val - val);
        origin_val
    }
}

#[derive(Debug)]
struct NotifyShared {
    task: Rc<RefCell<Option<Task>>>,
    count: Rc<Cell<u16>>,
    expect: u16,
}

impl Clone for NotifyShared {
    fn clone(&self) -> NotifyShared {
        let count = self.count.clone();
        let now_val = count.get();
        count.set(now_val + 1);
        NotifyShared {
            count,
            task: self.task.clone(),
            expect: self.expect,
        }
    }
}
