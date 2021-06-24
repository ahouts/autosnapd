use chrono::{DateTime, Local, Timelike};
#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait Clock {
    fn current(&self) -> DateTime<Local>;
}

pub struct ClockImpl;

impl Clock for ClockImpl {
    fn current(&self) -> DateTime<Local> {
        Local::now().with_nanosecond(0).unwrap()
    }
}
