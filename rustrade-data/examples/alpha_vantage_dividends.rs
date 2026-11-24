#![allow(clippy::unwrap_used, clippy::expect_used)] // Example code: panics acceptable for demonstration

//! A company's cash dividends and stock splits from Alpha Vantage, and what a backtest would have
//! known of its dividends on a past day.
//!
//! # Data use
//!
//! **Alpha Vantage's data is for personal, non-commercial use only.** Use by or for a firm, and
//! giving others access to the data, need a commercial licence from Alpha Vantage. Do not commit,
//! publish or redistribute what this prints. See <https://www.alphavantage.co/terms_of_service/>.
//!
//! # Running
//!
//! Get a free key at <https://www.alphavantage.co/support/#api-key>, then:
//!
//! ```bash
//! ALPHA_VANTAGE_API_KEY=<your key> \
//!   cargo run -p rustrade-data --example alpha_vantage_dividends --features alpha-vantage -- IBM
//! ```
//!
//! Two requests, of a free key's 25 a day.
//!
//! # Properties worth knowing before you build on this
//!
//! - **Amounts are as paid**, not adjusted for later splits.
//! - **A dividend is known from the end of its declaration date.** Before about 2016 Alpha
//!   Vantage gives no declaration date, so those dividends are known only from their ex-date.
//! - **Splits have no declaration date**: they are known only once they take effect.

use chrono::{Days, Utc};
use futures::StreamExt;
use rustrade_data::{dividends::DividendStore, exchange::alpha_vantage::AlphaVantageClient};
use rustrade_integration::corporate_action::{CorporateActionFilter, StockSplitSource};

#[tokio::main]
async fn main() {
    let symbol = std::env::args().nth(1).unwrap_or_else(|| "IBM".to_owned());
    let key = std::env::var("ALPHA_VANTAGE_API_KEY").expect("set ALPHA_VANTAGE_API_KEY");
    let client = AlphaVantageClient::new(&key).expect("API key");

    let dividends = client.dividends(&symbol).await.expect("dividends");
    println!("{symbol}: {} dividends; the last four:", dividends.len());
    for dividend in dividends.iter().rev().take(4).rev() {
        println!(
            "  ex {}  {} per share  declared {:?}  (known by {})",
            dividend.ex_date, dividend.amount, dividend.declaration_date, dividend.known_at
        );
    }

    let mut store = DividendStore::new("America/New_York".parse().unwrap());
    store.extend(dividends);
    let now = Utc::now();
    let a_year_ago = now - Days::new(365);
    for (label, as_of) in [("now", now), ("a year ago", a_year_ago)] {
        let next = store.next(&symbol, as_of);
        let previous = store.previous(&symbol, as_of);
        println!(
            "As of {label}: next ex {:?}, previous ex {:?}",
            next.map(|d| d.ex_date),
            previous.map(|d| d.ex_date)
        );
    }

    let filter = CorporateActionFilter::new(vec![symbol.as_str().into()], None, None);
    let splits: Vec<_> = client.fetch_splits(&filter).collect().await;
    println!("{symbol}: {} splits", splits.len());
    for split in splits {
        let split = split.expect("splits");
        println!("  effective {:?}  {:?}", split.effective_date, split.kind);
    }
}
