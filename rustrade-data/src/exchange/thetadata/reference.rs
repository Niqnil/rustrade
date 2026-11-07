//! Interest rates and the US equity market calendar.

use super::{
    client::{MAX_DAYS_DAILY, ThetaDataClient},
    error::ThetaDataError,
    model::Row,
};
use chrono::{NaiveDate, NaiveTime};
use futures::{Stream, StreamExt};
use rust_decimal::Decimal;

/// An interest-rate series ThetaData serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum InterestRateSeries {
    /// The Secured Overnight Financing Rate.
    Sofr,
    /// US Treasury yields at constant maturities of 1, 3 and 6 months, and 1, 2, 3, 5, 7, 10, 20
    /// and 30 years.
    TreasuryM1,
    TreasuryM3,
    TreasuryM6,
    TreasuryY1,
    TreasuryY2,
    TreasuryY3,
    TreasuryY5,
    TreasuryY7,
    TreasuryY10,
    TreasuryY20,
    TreasuryY30,
}

impl InterestRateSeries {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Sofr => "sofr",
            Self::TreasuryM1 => "treasury_m1",
            Self::TreasuryM3 => "treasury_m3",
            Self::TreasuryM6 => "treasury_m6",
            Self::TreasuryY1 => "treasury_y1",
            Self::TreasuryY2 => "treasury_y2",
            Self::TreasuryY3 => "treasury_y3",
            Self::TreasuryY5 => "treasury_y5",
            Self::TreasuryY7 => "treasury_y7",
            Self::TreasuryY10 => "treasury_y10",
            Self::TreasuryY20 => "treasury_y20",
            Self::TreasuryY30 => "treasury_y30",
        }
    }
}

/// One day's published rate of a series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InterestRate {
    pub date: NaiveDate,
    /// The rate in percent, as published: `3.91` is 3.91%.
    pub rate_percent: Decimal,
}

impl InterestRate {
    /// The rate as a fraction: `0.0391` for 3.91%.
    pub fn rate(&self) -> Decimal {
        self.rate_percent / Decimal::ONE_HUNDRED
    }
}

/// What the US equity market does on a day.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MarketDayKind {
    /// A regular session.
    Open,
    /// A shortened session, normally closing at 13:00 New York time. Options eligible to trade
    /// past the close trade until 13:15.
    EarlyClose,
    /// A holiday: no session.
    FullClose,
    /// A Saturday or Sunday.
    Weekend,
}

/// The US equity market's schedule on a day, in New York time.
///
/// `open` and `close` are the regular session's; `None` where ThetaData publishes none (a closed
/// day, and sometimes an early-close day, whose times it does not always fill in).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarketDay {
    pub kind: MarketDayKind,
    pub open: Option<NaiveTime>,
    pub close: Option<NaiveTime>,
}

/// A day the market is closed or closes early.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarketHoliday {
    pub date: NaiveDate,
    pub day: MarketDay,
}

fn decode_market_day(row: &Row<'_>) -> Result<MarketDay, ThetaDataError> {
    let kind = match row.str("type")?.as_ref() {
        "open" => MarketDayKind::Open,
        "early_close" => MarketDayKind::EarlyClose,
        "full_close" => MarketDayKind::FullClose,
        "weekend" => MarketDayKind::Weekend,
        other => return Err(row.error(format_args!("unknown market day type {other:?}"))),
    };
    Ok(MarketDay {
        kind,
        open: row.time_of_day_opt("open")?,
        close: row.time_of_day_opt("close")?,
    })
}

impl ThetaDataClient {
    /// Daily rates of `series` over `start..=end` (free plan). Days without a publication have no
    /// row.
    pub fn interest_rate_history(
        &self,
        series: InterestRateSeries,
        start: NaiveDate,
        end: NaiveDate,
    ) -> impl Stream<Item = Result<InterestRate, ThetaDataError>> + Send + use<> {
        self.ranged_rows(
            "interest_rate/history/eod".into(),
            vec![("symbol", series.as_str().to_owned())],
            (start, end),
            MAX_DAYS_DAILY,
            |row: &Row<'_>| {
                Ok(InterestRate {
                    date: row.date("created")?,
                    rate_percent: row.decimal("rate")?,
                })
            },
        )
    }

    /// Today's market schedule (free plan).
    pub async fn market_day_today(&self) -> Result<MarketDay, ThetaDataError> {
        self.single_row("calendar/today", Vec::new(), decode_market_day)
            .await
    }

    /// The market schedule on `date`, from 2012 through the end of next year (value plan).
    pub async fn market_day(&self, date: NaiveDate) -> Result<MarketDay, ThetaDataError> {
        self.single_row(
            "calendar/on_date",
            vec![("date", super::time::format_date(date))],
            decode_market_day,
        )
        .await
    }

    /// The days of `year` the market is closed or closes early, from 2012 through next year
    /// (value plan).
    pub fn market_holidays(
        &self,
        year: i32,
    ) -> impl Stream<Item = Result<MarketHoliday, ThetaDataError>> + Send + use<> {
        self.rows(
            "calendar/year_holidays".into(),
            vec![("year", year.to_string())],
            |row| {
                Ok(MarketHoliday {
                    date: row.date("date")?,
                    day: decode_market_day(row)?,
                })
            },
        )
    }

    async fn single_row<T: Send + 'static>(
        &self,
        path: &'static str,
        params: Vec<(&'static str, String)>,
        decode: fn(&Row<'_>) -> Result<T, ThetaDataError>,
    ) -> Result<T, ThetaDataError> {
        let rows = self.rows(path.into(), params, decode);
        futures::pin_mut!(rows);
        match rows.next().await {
            Some(row) => row,
            None => Err(ThetaDataError::Api {
                status: 200,
                message: format!("{path} returned no row"),
            }),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn market_days_decode() {
        let day = decode_market_day(
            &Row::parse(r#"{"type":"early_close","open":"09:30:00","close":"13:00:00"}"#).unwrap(),
        )
        .unwrap();
        assert_eq!(
            day,
            MarketDay {
                kind: MarketDayKind::EarlyClose,
                open: NaiveTime::from_hms_opt(9, 30, 0),
                close: NaiveTime::from_hms_opt(13, 0, 0),
            }
        );
        let day = decode_market_day(
            &Row::parse(r#"{"type":"full_close","open":null,"close":null}"#).unwrap(),
        )
        .unwrap();
        assert_eq!(
            (day.kind, day.open, day.close),
            (MarketDayKind::FullClose, None, None)
        );
        let day =
            decode_market_day(&Row::parse(r#"{"type":"weekend","open":"","close":""}"#).unwrap())
                .unwrap();
        assert_eq!(
            (day.kind, day.open, day.close),
            (MarketDayKind::Weekend, None, None)
        );
        assert_eq!(
            decode_market_day(
                &Row::parse(r#"{"type":"open","open":"09:30:00","close":"16:00:00"}"#).unwrap()
            )
            .unwrap()
            .kind,
            MarketDayKind::Open
        );
        assert!(decode_market_day(&Row::parse(r#"{"type":"halt"}"#).unwrap()).is_err());
    }

    #[test]
    fn a_rate_converts_from_percent() {
        let rate = InterestRate {
            date: NaiveDate::from_ymd_opt(2025, 11, 5).unwrap(),
            rate_percent: dec!(3.91),
        };
        assert_eq!(rate.rate(), dec!(0.0391));
    }

    #[test]
    fn every_series_has_its_parameter() {
        assert_eq!(InterestRateSeries::Sofr.as_str(), "sofr");
        assert_eq!(InterestRateSeries::TreasuryM1.as_str(), "treasury_m1");
        assert_eq!(InterestRateSeries::TreasuryM3.as_str(), "treasury_m3");
        assert_eq!(InterestRateSeries::TreasuryM6.as_str(), "treasury_m6");
        assert_eq!(InterestRateSeries::TreasuryY1.as_str(), "treasury_y1");
        assert_eq!(InterestRateSeries::TreasuryY2.as_str(), "treasury_y2");
        assert_eq!(InterestRateSeries::TreasuryY3.as_str(), "treasury_y3");
        assert_eq!(InterestRateSeries::TreasuryY5.as_str(), "treasury_y5");
        assert_eq!(InterestRateSeries::TreasuryY7.as_str(), "treasury_y7");
        assert_eq!(InterestRateSeries::TreasuryY10.as_str(), "treasury_y10");
        assert_eq!(InterestRateSeries::TreasuryY20.as_str(), "treasury_y20");
        assert_eq!(InterestRateSeries::TreasuryY30.as_str(), "treasury_y30");
    }
}
