use chrono::{DateTime, Timelike, Utc};
#[cfg(test)]
use mockall::automock;

#[cfg_attr(test, automock)]
pub trait Clock {
    fn current(&self) -> DateTime<Utc>;
}

pub struct ClockImpl;

impl Clock for ClockImpl {
    fn current(&self) -> DateTime<Utc> {
        Utc::now().with_nanosecond(0).unwrap()
    }
}
