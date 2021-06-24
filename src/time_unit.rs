use anyhow::{anyhow, Error};
use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone};
use std::fmt::{Display, Formatter};
use std::ops::Add;
use std::str::FromStr;
use strum_macros::EnumIter;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, EnumIter)]
pub enum TimeUnit {
    Minute,
    Hour,
    Day,
    Month,
    Year,
}

impl FromStr for TimeUnit {
    type Err = Error;

    fn from_str(s: &str) -> anyhow::Result<Self, Self::Err> {
        use TimeUnit::*;
        match s {
            "minutely" => Ok(Minute),
            "hourly" => Ok(Hour),
            "daily" => Ok(Day),
            "monthly" => Ok(Month),
            "yearly" => Ok(Year),
            _ => Err(anyhow!("unknown time unit: {}", s)),
        }
    }
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeUnit::Minute => write!(f, "minutely"),
            TimeUnit::Hour => write!(f, "hourly"),
            TimeUnit::Day => write!(f, "daily"),
            TimeUnit::Month => write!(f, "monthly"),
            TimeUnit::Year => write!(f, "yearly"),
        }
    }
}

impl<Tz: TimeZone> Add<TimeUnit> for DateTime<Tz> {
    type Output = DateTime<Tz>;

    fn add(self, time_unit: TimeUnit) -> Self::Output {
        fn number_of_days_in_month(year: i32, month: u32) -> i64 {
            if month == 12 {
                NaiveDate::from_ymd(year + 1, 1, 1)
            } else {
                NaiveDate::from_ymd(year, month + 1, 1)
            }
            .signed_duration_since(NaiveDate::from_ymd(year, month, 1))
            .num_days()
        }

        fn number_of_days_in_year(year: i32) -> i64 {
            NaiveDate::from_ymd(year + 1, 1, 1)
                .signed_duration_since(NaiveDate::from_ymd(year, 1, 1))
                .num_days()
        }

        match time_unit {
            TimeUnit::Minute => self.add(Duration::minutes(1)),
            TimeUnit::Hour => self.add(Duration::hours(1)),
            TimeUnit::Day => self.add(Duration::days(1)),
            TimeUnit::Month => {
                let year = self.year();
                let month = self.month();
                self.add(Duration::days(number_of_days_in_month(year, month)))
            }
            TimeUnit::Year => {
                let year = self.year();
                self.add(Duration::days(number_of_days_in_year(year)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use strum::IntoEnumIterator;

    #[test]
    fn round_trip() {
        for time_unit in TimeUnit::iter() {
            assert_eq!(
                time_unit,
                TimeUnit::from_str(format!("{}", time_unit).as_str()).unwrap()
            )
        }
    }

    #[test]
    fn add_month() {
        let dt = Utc
            .from_local_datetime(&NaiveDateTime::new(
                NaiveDate::from_ymd(2021, 11, 12),
                NaiveTime::from_hms(2, 3, 4),
            ))
            .unwrap();
        assert_eq!(12, (dt + TimeUnit::Month).month())
    }

    #[test]
    fn add_month_year_rollover() {
        let dt = Utc
            .from_local_datetime(&NaiveDateTime::new(
                NaiveDate::from_ymd(2021, 12, 12),
                NaiveTime::from_hms(2, 3, 4),
            ))
            .unwrap();
        assert_eq!(1, (dt + TimeUnit::Month).month());
        assert_eq!(2022, (dt + TimeUnit::Month).year());
    }

    #[test]
    fn add_year_skip_year() {
        let dt = Utc
            .from_local_datetime(&NaiveDateTime::new(
                NaiveDate::from_ymd(2024, 12, 31),
                NaiveTime::from_hms(2, 3, 4),
            ))
            .unwrap();
        assert_eq!(
            NaiveDate::from_ymd(2026, 1, 1),
            (dt + TimeUnit::Year).date().naive_utc()
        )
    }
}
