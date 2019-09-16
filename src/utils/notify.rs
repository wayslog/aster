use futures::task::Task;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug, Clone)]
pub struct Notify {
    task: Rc<RefCell<Option<Task>>>,
}

impl Notify {
    pub fn empty() -> Self {
        Notify {
            task: Rc::new(RefCell::new(None)),
        }
    }

    pub fn set_task(&mut self, task: Task) {
        self.task.borrow_mut().replace(task);
    }

    pub fn notify(&self) {
        if let Some(task) = self.task.borrow().as_ref() {
            trace!("trace notify");
            task.notify();
        } else {
            trace!("trace notify None");
        }
    }
}
