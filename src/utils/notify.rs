use futures::task::Task;

#[derive(Debug, Clone)]
pub struct Notify {
    task: Option<Task>,
    expect: usize,
}

impl Notify {
    pub fn empty() -> Self {
        Notify {
            task: None,
            expect: std::usize::MAX,
        }
    }

    pub fn set_expect(&mut self, num: usize) {
        self.expect = num;
    }

    pub fn expect(&self) -> usize {
        self.expect
    }

    pub fn set_task(&mut self, task: Task) {
        self.task = Some(task);
    }

    pub fn notify(&self) {
        if let Some(task) = self.task.as_ref() {
            trace!("trace notify");
            task.notify();
        } else {
            trace!("trace notify None");
        }
    }
}
