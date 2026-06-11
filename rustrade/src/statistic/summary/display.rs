use crate::statistic::{
    summary::{TradingSummary, asset::TearSheetAsset, instrument::TearSheet},
    time::TimeInterval,
};
use prettytable::{Cell, Row, Table};
use rust_decimal::Decimal;

impl<Interval> TradingSummary<Interval>
where
    Interval: TimeInterval,
{
    pub fn print_summary(&self) {
        println!();
        self.title_table().printstd();
        self.instrument_table().printstd();
        self.asset_table().printstd();
    }
    fn title_table(&self) -> Table {
        let mut title_table = Table::new();
        title_table.set_format(*prettytable::format::consts::FORMAT_CLEAN);

        // ASCII art for "TRADING SUMMARY"
        let large_text = vec![
            "████████ ██████   █████  ██████  ██ ███    ██  ██████      ███████ ██    ██ ███    ███ ███    ███  █████  ██████  ██    ██",
            "   ██    ██   ██ ██   ██ ██   ██ ██ ████   ██ ██           ██      ██    ██ ████  ████ ████  ████ ██   ██ ██   ██  ██  ██ ",
            "   ██    ██████  ███████ ██   ██ ██ ██ ██  ██ ██   ███     ███████ ██    ██ ██ ████ ██ ██ ████ ██ ███████ ██████    ████  ",
            "   ██    ██   ██ ██   ██ ██   ██ ██ ██  ██ ██ ██    ██          ██ ██    ██ ██  ██  ██ ██  ██  ██ ██   ██ ██   ██    ██   ",
            "   ██    ██   ██ ██   ██ ██████  ██ ██   ████  ██████      ███████  ██████  ██      ██ ██      ██ ██   ██ ██   ██    ██   ",
        ];

        // Add the large text
        for line in large_text {
            let mut cell = Cell::new(line).style_spec("bB"); // Bold and blue
            cell.set_hspan(1);
            title_table.add_row(Row::new(vec![cell]));
        }

        let mut duration_cell = Cell::new(&{
            let duration = self.trading_duration();
            let minutes = duration.num_minutes();
            let hours = duration.num_hours();
            let days = duration.num_days();

            match (minutes, hours, days) {
                // Less than 1 hour - show minutes
                (minutes, _, _) if minutes < 60 => {
                    format!("(Trading Duration: {minutes} Minutes)")
                }

                // Between 1 hour and 24 hours - show hours and minutes
                (minutes, hours, _) if hours < 24 => {
                    let remaining_minutes = minutes % 60;
                    format!("(Trading Duration: {hours} Hour(s) & {remaining_minutes} Minutes)")
                }

                // Between 1 day and 7 days - show days and hours
                (_, hours, days) if days <= 7 => {
                    let remaining_hours = hours % 24;
                    format!("(Trading Duration: {days} Day(s) & {remaining_hours} Hour(s))")
                }

                // Between 1 week and 4 weeks - show weeks and days
                (_, _, days) if days <= 28 => {
                    let weeks = days / 7;
                    let remaining_days = days % 7;
                    format!("(Trading Duration: {weeks} Week(s) & {remaining_days} Day(s))")
                }

                // More than 4 weeks - show only days with ~ indicator
                (_, _, days) => {
                    format!("(Trading Duration: ~{days} Days)")
                }
            }
        })
        .style_spec("bcB");

        duration_cell.set_hspan(1);
        title_table.add_row(Row::new(vec![duration_cell]));

        title_table
    }

    pub fn instrument_table(&self) -> Table {
        let mut table = Table::new();

        // Styling
        table.set_format(*prettytable::format::consts::FORMAT_BOX_CHARS);

        // Title row spanning all columns
        let num_columns = self.instruments.len() + 1;
        let mut title_row = Row::new(vec![]);
        let mut title_cell = Cell::new("Instrument TearSheets").style_spec("bcB");
        title_cell.set_hspan(num_columns);
        title_row.add_cell(title_cell);
        table.add_row(title_row);

        // Extract TimeInterval name (eg/ Annual365, Daily, etc)
        let interval = match self.instruments.first() {
            Some((_, sheet)) => sheet.sharpe_ratio.interval.name(),
            None => return table,
        };

        // Header row (eg/ Metric | bybit_btc_usdt | okx_eth_usdt | ... )
        let mut header_row = Row::new(vec![Cell::new("").style_spec("bcB")]);
        for instrument in self.instruments.keys() {
            header_row.add_cell(Cell::new(instrument.name().as_str()).style_spec("bcB"));
        }
        table.add_row(header_row);

        // Add metric rows
        self.add_instrument_metric_row(&mut table, "PnL", |ts| format!("{:.2}", ts.pnl));
        self.add_instrument_metric_row(&mut table, &format!("Return {interval}"), |ts| {
            format_percentage(ts.pnl_return.value, 2)
        });
        self.add_instrument_metric_row(&mut table, &format!("Sharpe {interval}"), |ts| {
            format_ratio(ts.sharpe_ratio.value)
        });
        self.add_instrument_metric_row(&mut table, &format!("Sortino {interval}"), |ts| {
            format_ratio(ts.sortino_ratio.value)
        });
        self.add_instrument_metric_row(&mut table, &format!("Calmar {interval}"), |ts| {
            format_ratio(ts.calmar_ratio.value)
        });
        self.add_instrument_metric_row(&mut table, "PnL Drawdown", |ts| {
            if let Some(drawdown) = &ts.pnl_drawdown {
                format_percentage(drawdown.value, 2)
            } else {
                "N/A".to_string()
            }
        });
        self.add_instrument_metric_row(&mut table, "PnL Drawdown Avg", |ts| {
            if let Some(mean_drawdown) = &ts.pnl_drawdown_mean {
                format_percentage(mean_drawdown.mean_drawdown, 2)
            } else {
                "N/A".to_string()
            }
        });
        self.add_instrument_metric_row(&mut table, "PnL Drawdown Max", |ts| {
            if let Some(max_drawdown) = &ts.pnl_drawdown_max {
                format_percentage(max_drawdown.0.value, 2)
            } else {
                "N/A".to_string()
            }
        });
        self.add_instrument_metric_row(&mut table, "Win Rate", |ts| {
            if let Some(win_rate) = &ts.win_rate {
                format_percentage(win_rate.value, 1)
            } else {
                "N/A".to_string()
            }
        });
        self.add_instrument_metric_row(&mut table, "Profit Factor", |ts| {
            if let Some(profit_factor) = &ts.profit_factor {
                format!("{:.2}", profit_factor.value)
            } else {
                "N/A".to_string()
            }
        });

        table
    }

    fn add_instrument_metric_row<F>(&self, table: &mut Table, label: &str, format_value: F)
    where
        F: Fn(&TearSheet<Interval>) -> String,
    {
        let mut row = Row::new(vec![Cell::new(label).style_spec("bcB")]);
        for tear_sheet in self.instruments.values() {
            row.add_cell(Cell::new(&format_value(tear_sheet)));
        }
        table.add_row(row);
    }

    pub fn asset_table(&self) -> Table {
        let mut table = Table::new();

        // Styling
        table.set_format(*prettytable::format::consts::FORMAT_BOX_CHARS);

        // Title row spanning all columns
        let num_columns = self.assets.len() + 1;
        let mut title_row = Row::new(vec![]);

        let mut title_cell = Cell::new("Asset TearSheets").style_spec("bcB");
        title_cell.set_hspan(num_columns);
        title_row.add_cell(title_cell);
        table.add_row(title_row);

        // Header row (eg/ Metric | btc | eth | ...)
        let mut header_row = Row::new(vec![Cell::new("").style_spec("bcB")]);
        for asset in self.assets.keys() {
            header_row.add_cell(
                Cell::new(&format!("{}_{}", asset.exchange.as_str(), asset.asset))
                    .style_spec("bcB"),
            );
        }
        table.add_row(header_row);

        // Add metric rows. The "Balance" row reflects the session's BalanceBasis in both its label
        // and its value, so a net-asset summary is never mistaken for a gross one.
        let basis = self.basis;
        self.add_asset_metric_row(&mut table, &format!("Balance ({basis})"), move |ts| {
            if let Some(balance) = ts.balance_end {
                format!("{:.8}", basis.value(balance))
            } else {
                "N/A".to_string()
            }
        });
        self.add_asset_metric_row(&mut table, "Drawdown", |ts| {
            if let Some(drawdown) = &ts.drawdown {
                format_percentage(drawdown.value, 2)
            } else {
                "N/A".to_string()
            }
        });
        self.add_asset_metric_row(&mut table, "Drawdown Avg", |ts| {
            if let Some(mean_drawdown) = &ts.drawdown_mean {
                format_percentage(mean_drawdown.mean_drawdown, 2)
            } else {
                "N/A".to_string()
            }
        });
        self.add_asset_metric_row(&mut table, "Drawdown Max", |ts| {
            if let Some(max_drawdown) = &ts.drawdown_max {
                format_percentage(max_drawdown.0.value, 2)
            } else {
                "N/A".to_string()
            }
        });

        table
    }

    fn add_asset_metric_row<F>(&self, table: &mut Table, label: &str, format_value: F)
    where
        F: Fn(&TearSheetAsset) -> String,
    {
        let mut row = Row::new(vec![Cell::new(label).style_spec("bcB")]);
        for tear_sheet in self.assets.values() {
            row.add_cell(Cell::new(&format_value(tear_sheet)));
        }
        table.add_row(row);
    }
}

fn format_ratio(value: Decimal) -> String {
    if value == Decimal::MAX {
        "∞".to_string()
    } else if value == Decimal::MIN {
        "-∞".to_string()
    } else {
        format!("{value:.4}")
    }
}

fn format_percentage(value: Decimal, precision: usize) -> String {
    match value.checked_mul(Decimal::ONE_HUNDRED) {
        Some(pct) => format!("{pct:.precision$}%"),
        None => "∞%".to_string(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::statistic::{summary::asset::BalanceBasis, time::Annual365};
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;
    use rustrade_execution::balance::Balance;
    use rustrade_instrument::{
        asset::{ExchangeAsset, name::AssetNameInternal},
        exchange::ExchangeId,
    };
    use rustrade_integration::collection::FnvIndexMap;

    fn summary_with_basis(basis: BalanceBasis, balance_end: Balance) -> TradingSummary<Annual365> {
        let mut assets = FnvIndexMap::default();
        assets.insert(
            ExchangeAsset::new(ExchangeId::BinanceSpot, AssetNameInternal::new("btc")),
            TearSheetAsset {
                balance_end: Some(balance_end),
                drawdown: None,
                drawdown_mean: None,
                drawdown_max: None,
            },
        );

        TradingSummary {
            time_engine_start: DateTime::<Utc>::MIN_UTC,
            time_engine_end: DateTime::<Utc>::MIN_UTC,
            instruments: FnvIndexMap::default(),
            assets,
            basis,
        }
    }

    /// The asset-table "Balance" row labels itself with the session basis and formats the value
    /// through that basis, so a net-asset summary (net 30, gross 180) is never read as gross.
    #[test]
    fn asset_table_balance_row_reflects_net_asset_basis() {
        // total 180, net asset 30 (borrowed 150).
        let margin = Balance::new_margin(dec!(180.0), dec!(30.0), dec!(150.0), dec!(0.0));
        let rendered = summary_with_basis(BalanceBasis::NetAsset, margin)
            .asset_table()
            .to_string();

        assert!(rendered.contains("Balance (net asset)"));
        assert!(rendered.contains("30.00000000")); // net asset, not gross total
        assert!(!rendered.contains("Balance (gross)"));
    }

    /// Under the default Gross basis the row labels itself "Balance (gross)" and shows the raw total.
    #[test]
    fn asset_table_balance_row_reflects_gross_basis() {
        let margin = Balance::new_margin(dec!(180.0), dec!(30.0), dec!(150.0), dec!(0.0));
        let rendered = summary_with_basis(BalanceBasis::Gross, margin)
            .asset_table()
            .to_string();

        assert!(rendered.contains("Balance (gross)"));
        assert!(rendered.contains("180.00000000")); // gross total
    }
}
