//! Decode a downloaded bulk-export artifact into [`MarketEvent`]s.
//!
//! Behind the `lse-parquet` feature: the core `lse` feature runs export jobs and produces files
//! without any Parquet dependency, so a consumer who only wants artifacts on disk pays nothing for
//! this.
//!
//! # The item type is decided by the file, not by the caller
//! The provider's tick schema **varies by dataset**, so there is no single "LSE tick" event. All
//! four layouts below are measured, and the decoder dispatches on the columns actually present:
//!
//! One qualification on "the columns actually present": candle-versus-tick comes from the
//! [`LseExportTimeframe`] the caller ran the job with, and only the tick sub-dispatch reads the
//! schema. This is not a fallback if the two disagree — a tick export whose file carries OHLC
//! columns is rejected as [`LseError::UnsupportedSchema`] rather than silently decoded as candles,
//! so a mismatch fails observably in either direction.
//!
//! ```text
//! bid,   ask                      => DataKind::OrderBookL1   (fx)
//! price, ask   (+ volume)         => DataKind::OrderBookL1   (volatility, interest_rates, currency_index)
//! price, volume                   => DataKind::Trade         (stocks, and the other candle classes)
//! open, high, low, close (+ vol)  => DataKind::Candle
//! ```
//!
//! `price` is the **bid**: the provider's own price endpoint returns `price`, `bid` and `ask` side
//! by side and `price == bid` exactly, measured on ten symbols spanning every dataset family. That
//! is why the `price`/`ask` layout decodes to a quote rather than a trade.
//!
//! # ⚠️ A decoded [`DataKind::Trade`] may not be a trade
//! The `price`/`volume` layout carries no ask, so a quote is not constructible and a trade is the
//! only available mapping — but by the measurement above, that `price` is very likely a bid-side
//! observation rather than a print. Treat it as "the provider's price series", not as evidence a
//! trade occurred at that instant. The same caveat reaches candles built from it.
//!
//! # ⚠️ Candles are BID candles, at least for FX
//! Reconciling a day of `EUR/USD` 1-minute candles against the tick tape for the same day, open,
//! high, low and close matched the **bid** series on 1421 of 1421 minutes and matched the mid or
//! the ask on none. A backtest that fills at the candle close is filling at the bid — favourable
//! by a full spread on every buy. This is a property of the data, not of this decoder, and it
//! cannot be corrected here without inventing a spread.
//!
//! # ⚠️ Volume is not a dependable figure
//! The candle `volume` column is **absent entirely** for FX (the true `None`) and present for
//! equities — but measured unreliable there: a majority of one-minute bars reported `0` in minutes
//! where the tick tape shows real trades, and a daily series carried a contiguous band roughly
//! 2,000× too large. A literal `0` is decoded faithfully as `Some(0)` rather than being rewritten
//! to `None`: rewriting would be this library inventing a fact. Validate before using it.
//!
//! # One file, one symbol
//! Every artifact the provider will produce is single-symbol — `symbol` is mandatory on an export
//! and `"all"` silently matches nothing. So **combining instruments needs
//! [`merge_time_sorted`](crate::streams::merge::merge_time_sorted)**, as on the candle replay path;
//! there is no multi-symbol file to decode in one pass. See [`read_export`] for the adapter that
//! bridges a decoded artifact to that merge. The decoder still verifies the `symbol` column on
//! every row, which is what catches a mis-described file — including the `BP` versus `BP.L` case,
//! where the two are different instruments in different currencies.
//!
//! # ⚠️ Licensing
//! Decoded data is **not redistributable**. See the [module documentation](super) and
//! <https://londonstrategicedge.com/terms>.

use crate::books::Level;
use crate::event::{DataKind, MarketEvent};
use crate::exchange::lse::error::LseError;
use crate::exchange::lse::export::{LseExport, LseExportTimeframe};
use crate::subscription::book::OrderBookL1;
use crate::subscription::candle::{Candle, CandleInterval, IntervalStep, close_time_from_open};
use crate::subscription::trade::PublicTrade;
use chrono::{DateTime, Utc};
use parquet::basic::{ConvertedType, LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{ByteArray, ByteArrayType, DataType, DoubleType, Int64Type};
use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader, RowGroupReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use parquet::schema::types::{Type as SchemaType, TypePtr};
use rust_decimal::Decimal;
use rustrade_instrument::{exchange::ExchangeId, instrument::InstrumentIndex};
use smol_str::SmolStr;
use std::fs::File;
use std::path::Path;
use tracing::{info, warn};

/// Column name of the event timestamp, present on every measured layout.
const COL_TS: &str = "ts";
/// Column name of the instrument symbol, present on every measured layout — even single-symbol.
const COL_SYMBOL: &str = "symbol";

/// Rows decoded per column-reader call.
///
/// Bounds the decoder's working set independently of how the artifact was written: the buffers are
/// this wide, not one row group wide, so a writer using million-row groups costs the same here as one
/// using sixty-four-thousand-row groups. That is what keeps
/// [`MarketDataStreamed`](https://docs.rs/rustrade)'s bounded-memory contract true over a
/// multi-gigabyte tape. Large enough to amortise the per-call overhead, small enough that every
/// buffer together stays well inside a megabyte.
const BATCH_ROWS: usize = 8 * 1024;

/// The most value columns any layout names: OHLC plus `volume`.
///
/// One row's values are extracted into an array this wide, so the decode allocates nothing per row.
/// `test_no_layout_exceeds_the_value_column_budget` holds this in step with the layouts.
const MAX_VALUE_COLUMNS: usize = 5;

/// The row layout of an export artifact, resolved from the columns present.
///
/// The indices are **slots** into the decoder's per-row value array, not column positions in the
/// file: [`ColumnPlan`] maps each slot to the leaf column it reads. So a column no layout names is
/// never read at all, and the projection is a consequence of the layout rather than a second list to
/// keep in step with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowLayout {
    /// `bid` + `ask` — an explicit two-sided quote.
    Quote { bid: usize, ask: usize },
    /// `price` + `ask`, where `price` is the bid.
    ///
    /// `volume` is present in these schemas but carries no information: it was measured as a
    /// literal `0.0` on every row of the one synthetic dataset sampled — never null, though the
    /// column *is* nullable, so `None` is expressible and a different dataset may well use it. An
    /// L1 quote has nowhere to put a single undifferentiated size anyway: it cannot be split into a
    /// bid size and an ask size without inventing one.
    ///
    /// So the layout names no slot for it: it is **not decoded at all**. That the measurement still
    /// holds is checked from each row group's column-chunk statistics instead — see
    /// [`ColumnPlan::discarded_volume`]. The check keys on a **non-zero** value, because a zero is
    /// the provider doing exactly what it has always done and discarding it loses nothing — whereas
    /// a real size means the provider started populating a column this layout drops, which is a fact
    /// worth one `warn!` rather than a silent drop. Keying on non-null instead would warn on every
    /// artifact, which is noise rather than observability.
    QuoteFromPrice { price: usize, ask: usize },
    /// `price` + `volume` with no ask.
    Trade { price: usize, volume: usize },
    /// OHLC, with `volume` present only on some datasets.
    ///
    /// Carries the resolution it was resolved for, so deriving the bar's close needs no second look
    /// at the timeframe — and so "candle columns on a tick export" is unrepresentable rather than an
    /// error branch that cannot be reached.
    Candle {
        open: usize,
        high: usize,
        low: usize,
        close: usize,
        volume: Option<usize>,
        interval: CandleInterval,
    },
}

/// A leaf column the decoder reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LeafColumn {
    /// Its index among the schema's leaf columns — which, the schema being flat, is its field
    /// position.
    leaf: usize,
    /// Whether it can be null, i.e. whether its chunk carries definition levels.
    optional: bool,
}

/// Which columns of an artifact the decoder reads, and how the layout indexes them.
///
/// Built once per file, from the schema alone.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnPlan {
    layout: RowLayout,
    ts: LeafColumn,
    symbol: LeafColumn,
    /// The layout's value columns, indexed by its slots.
    values: Vec<LeafColumn>,
    /// A `volume` column present on a [`RowLayout::QuoteFromPrice`] artifact, which that layout
    /// discards.
    ///
    /// Held **outside** [`values`](Self::values) because it is never decoded. Its purpose is to
    /// notice the provider starting to populate a column this layout drops, and that is a fact about
    /// the artifact rather than about any row — so it is answered from each row group's column-chunk
    /// **statistics**, which the writer already computed, instead of by decoding the column.
    ///
    /// Decoding it cost one extra `DOUBLE` chunk decompressed and read end to end for at most one
    /// `warn!` line: roughly 400 MB of decode on a 50M-row artifact, and no early exit, since the
    /// column was read as part of the row batch whether or not the warning had already fired.
    discarded_volume: Option<LeafColumn>,
}

impl ColumnPlan {
    /// Resolve the layout and locate every column it reads, checking each one's type.
    ///
    /// # Errors
    /// - [`LseError::UnsupportedSchema`] if the schema is not flat and all-primitive, or if the
    ///   columns match none of the measured layouts. Surfaced up front rather than mis-decoded: a
    ///   wrong column guess would produce plausible numbers in the wrong field.
    /// - [`LseError::UnsupportedColumnType`] if a column the layout names is not the type this
    ///   decoder reads it as — including a `ts` that is not a **UTC-adjusted** microsecond timestamp,
    ///   which is otherwise indistinguishable from one that is.
    fn resolve(fields: &[TypePtr], timeframe: LseExportTimeframe) -> Result<Self, LseError> {
        let unsupported = || LseError::UnsupportedSchema {
            columns: column_names(fields),
        };

        // Leaf order equals field order only when every field is a primitive; see
        // `LseError::UnsupportedSchema`.
        if !fields.iter().all(|field| is_flat_primitive(field)) {
            return Err(unsupported());
        }

        let has = |name: &str| column(fields, name).is_some();

        // Present on every measured layout; their absence means this is not an export artifact.
        if !has(COL_TS) || !has(COL_SYMBOL) {
            return Err(unsupported());
        }

        // Each arm pairs a layout whose slots are positions in `names` with the names themselves, so
        // the two cannot drift apart the way an index list and a separate projection could.
        let (layout, names): (RowLayout, Vec<&str>) = match timeframe {
            LseExportTimeframe::Candle(interval) => {
                let mut names = vec!["open", "high", "low", "close"];
                if !names.iter().all(|name| has(name)) {
                    return Err(unsupported());
                }

                // Absent for FX, present for equities. Keyed on presence, never on arity.
                let mut volume = None;
                if has("volume") {
                    volume = Some(names.len());
                    names.push("volume");
                }

                (
                    RowLayout::Candle {
                        open: 0,
                        high: 1,
                        low: 2,
                        close: 3,
                        volume,
                        interval,
                    },
                    names,
                )
            }
            LseExportTimeframe::Tick => match (has("bid"), has("ask"), has("price")) {
                (true, true, _) => (RowLayout::Quote { bid: 0, ask: 1 }, vec!["bid", "ask"]),
                // `price` beside an `ask` is the bid, so this is a quote despite the column name.
                (false, true, true) => (
                    RowLayout::QuoteFromPrice { price: 0, ask: 1 },
                    vec!["price", "ask"],
                ),
                (false, false, true) if has("volume") => (
                    RowLayout::Trade {
                        price: 0,
                        volume: 1,
                    },
                    vec!["price", "volume"],
                ),
                _ => return Err(unsupported()),
            },
        };

        // Located and type-checked like any other column, but deliberately NOT in `values`: it is
        // never decoded. See `RowLayout::QuoteFromPrice` and `ColumnPlan::discarded_volume`.
        let discarded_volume = match (&layout, has("volume")) {
            (RowLayout::QuoteFromPrice { .. }, true) => Some(double_column(fields, "volume")?),
            _ => None,
        };

        Ok(Self {
            layout,
            ts: timestamp_column(fields, COL_TS)?,
            symbol: utf8_column(fields, COL_SYMBOL)?,
            values: names
                .into_iter()
                .map(|name| double_column(fields, name))
                .collect::<Result<_, _>>()?,
            discarded_volume,
        })
    }
}

/// Locate a top-level field by name.
fn column<'a>(fields: &'a [TypePtr], name: &str) -> Option<(usize, &'a TypePtr)> {
    fields
        .iter()
        .enumerate()
        .find(|(_, field)| field.name() == name)
}

/// The artifact's column names, in schema order, for a diagnostic.
fn column_names(fields: &[TypePtr]) -> String {
    fields
        .iter()
        .map(|field| field.name())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Whether `field` is a primitive that contributes exactly one leaf column, at its own position.
fn is_flat_primitive(field: &SchemaType) -> bool {
    let info = field.get_basic_info();

    field.is_primitive() && !(info.has_repetition() && info.repetition() == Repetition::REPEATED)
}

/// Whether a field can be null, i.e. whether its column chunk carries definition levels.
fn is_optional(field: &SchemaType) -> bool {
    let info = field.get_basic_info();

    info.has_repetition() && info.repetition() == Repetition::OPTIONAL
}

/// Locate a named column, or report that the schema is not one this decoder knows.
fn required_column<'a>(
    fields: &'a [TypePtr],
    name: &str,
) -> Result<(usize, &'a TypePtr), LseError> {
    column(fields, name).ok_or_else(|| LseError::UnsupportedSchema {
        columns: column_names(fields),
    })
}

/// Locate `name` and require it to be a `DOUBLE`.
fn double_column(fields: &[TypePtr], name: &str) -> Result<LeafColumn, LseError> {
    let (leaf, field) = required_column(fields, name)?;

    if field.get_physical_type() != PhysicalType::DOUBLE {
        return Err(LseError::UnsupportedColumnType {
            column: name.to_owned(),
            required: "DOUBLE",
            found: describe_type(field),
        });
    }

    Ok(LeafColumn {
        leaf,
        optional: is_optional(field),
    })
}

/// Locate `name` and require it to be a UTF-8 `BYTE_ARRAY`.
fn utf8_column(fields: &[TypePtr], name: &str) -> Result<LeafColumn, LseError> {
    let (leaf, field) = required_column(fields, name)?;
    let info = field.get_basic_info();

    let utf8 = matches!(info.logical_type_ref(), Some(LogicalType::String))
        || info.converted_type() == ConvertedType::UTF8;

    if field.get_physical_type() != PhysicalType::BYTE_ARRAY || !utf8 {
        return Err(LseError::UnsupportedColumnType {
            column: name.to_owned(),
            required: "BYTE_ARRAY (STRING)",
            found: describe_type(field),
        });
    }

    Ok(LeafColumn {
        leaf,
        optional: is_optional(field),
    })
}

/// Locate `name` and require it to be a **UTC-adjusted** microsecond timestamp.
///
/// The UTC flag is the load-bearing half; see [`LseError::UnsupportedColumnType`] for why a
/// local-time column read as epoch microseconds is undetectable downstream.
fn timestamp_column(fields: &[TypePtr], name: &str) -> Result<LeafColumn, LseError> {
    let (leaf, field) = required_column(fields, name)?;
    let info = field.get_basic_info();

    let utc_micros = match info.logical_type_ref() {
        Some(LogicalType::Timestamp(timestamp)) => {
            timestamp.is_adjusted_to_u_t_c && timestamp.unit == TimeUnit::MICROS
        }
        // A writer predating the logical-type union records only the converted type, which the
        // Parquet specification defines as UTC-normalised. `parquet` maps BOTH values of
        // `is_adjusted_to_utc` onto that same converted type, so it is evidence of UTC only when no
        // logical type is present to contradict it — hence this arm rather than a check on
        // `converted_type` alone, which would accept exactly the local-time column above.
        None => info.converted_type() == ConvertedType::TIMESTAMP_MICROS,
        Some(_) => false,
    };

    if field.get_physical_type() != PhysicalType::INT64 || !utc_micros {
        return Err(LseError::UnsupportedColumnType {
            column: name.to_owned(),
            required: "INT64 (TIMESTAMP(MICROS, is_adjusted_to_utc = true))",
            found: describe_type(field),
        });
    }

    Ok(LeafColumn {
        leaf,
        optional: is_optional(field),
    })
}

/// Render a field's declared type for a diagnostic.
fn describe_type(field: &SchemaType) -> String {
    let info = field.get_basic_info();
    let physical = field.get_physical_type();

    match (info.logical_type_ref(), info.converted_type()) {
        (Some(logical), _) => format!("{physical} ({logical:?})"),
        (None, ConvertedType::NONE) => physical.to_string(),
        (None, converted) => format!("{physical} ({converted:?})"),
    }
}

/// One leaf column of an artifact, read a batch of rows at a time into reusable buffers.
///
/// The buffers are the point. The record API this replaced allocated, on **every row**, an un-hinted
/// `Vec` of fields, one heap `String` per column for the column *name*, and a `String` for the
/// dictionary-encoded `symbol` that is identical on every row of a single-symbol file — a large
/// fraction of decode time in local profiling, though no in-tree benchmark pins the figure. Here each
/// column owns two buffers for the whole decode and a batch read appends into them.
struct BatchedColumn<T: DataType> {
    reader: ColumnReaderImpl<T>,
    /// Index of this leaf in the file schema. Carried only so an underrun can name the column it
    /// happened in — `BatchedColumn` is generic over the physical type, not the layout, so the
    /// human-readable name is not in scope here.
    leaf: usize,
    /// `true` when the column is `OPTIONAL`, so `values` is sparse and `def_levels` says which rows
    /// it covers.
    optional: bool,
    /// The batch's **non-null** values, in row order. A null row occupies no slot.
    values: Vec<T::T>,
    /// One definition level per row of the batch: `1` present, `0` null. Left empty for a `REQUIRED`
    /// column, which cannot be null.
    def_levels: Vec<i16>,
    /// Index into `values` of the next row's value.
    ///
    /// Rows are consumed strictly in order, so a cursor is enough. Locating row *i*'s value by
    /// counting the definition levels before it would make the decode quadratic in the batch size.
    cursor: usize,
}

impl<T: DataType> BatchedColumn<T> {
    /// Open a reader for `column`'s chunk in `row_group`.
    ///
    /// # Errors
    /// [`LseError::Parquet`] if the chunk cannot be opened, or if its physical type is not `T`'s — a
    /// state [`ColumnPlan::resolve`] has already ruled out, reported here rather than as the panic
    /// `parquet`'s own `get_typed_column_reader` would raise.
    fn open(row_group: &dyn RowGroupReader, column: LeafColumn) -> Result<Self, LseError> {
        let reader = row_group.get_column_reader(column.leaf)?;
        let reader = T::get_column_reader(reader).ok_or_else(|| {
            ParquetError::General(format!(
                "export column {} is not a {} column",
                column.leaf,
                T::get_physical_type()
            ))
        })?;

        Ok(Self {
            reader,
            leaf: column.leaf,
            optional: column.optional,
            values: Vec::new(),
            def_levels: Vec::new(),
            cursor: 0,
        })
    }

    /// Read up to [`BATCH_ROWS`] more rows, returning how many were read.
    ///
    /// Zero means this column chunk is exhausted.
    ///
    /// # Errors
    /// [`LseError::Parquet`] for any decode failure.
    fn read_batch(&mut self) -> Result<usize, LseError> {
        // `read_records` appends rather than overwrites, so the previous batch has to go first.
        self.values.clear();
        self.def_levels.clear();
        self.cursor = 0;

        // A column with definition levels rejects a call that supplies nowhere to put them, and one
        // without them ignores the buffer entirely — so the two cases cannot share a single call.
        let (rows, _values, _levels) = match self.optional {
            true => self.reader.read_records(
                BATCH_ROWS,
                Some(&mut self.def_levels),
                None,
                &mut self.values,
            )?,
            false => self
                .reader
                .read_records(BATCH_ROWS, None, None, &mut self.values)?,
        };

        Ok(rows)
    }

    /// Take the value at `row` of the buffered batch, or `Ok(None)` if that row is null.
    ///
    /// Must be called once per row per column, in row order, whether or not the layout reads the
    /// value: the cursor over the non-null values only advances by being taken.
    ///
    /// # Errors
    /// [`LseError::Parquet`] if the row is marked present but the batch holds no value for it —
    /// a malformed chunk, or a caller that skipped a row and desynchronised the cursor.
    ///
    /// Reported rather than folded into the `None` a genuine SQL null returns: the two mean
    /// opposite things. A fabricated null surfaces as [`LseError::NullValue`] on a required column,
    /// blaming the file for a fault that is not in it, and on a nullable one it is not surfaced at
    /// all — the row simply decodes with a missing `volume` or a one-sided book.
    fn take(&mut self, row: usize) -> Result<Option<T::T>, LseError> {
        let value = self.peek(row)?.cloned();
        self.advance(row);

        Ok(value)
    }

    /// Borrow the value at `row` without advancing.
    ///
    /// The half of [`take`](Self::take) that costs nothing. For a `BYTE_ARRAY` column the clone
    /// `take` performs is **not** a memcpy — [`ByteArray`] wraps an `Option<bytes::Bytes>`, so a
    /// dictionary-backed clone is an atomic increment and its drop an atomic decrement: two
    /// lock-prefixed read-modify-writes per row, on the order of 100M of them across a 50M-row
    /// export, for a value that is only compared against a `&[u8]` and dropped. Callers that just
    /// look at the value pair this with [`advance`](Self::advance) instead.
    ///
    /// # Errors
    /// As [`take`](Self::take).
    fn peek(&self, row: usize) -> Result<Option<&T::T>, LseError> {
        if self.is_null(row) {
            return Ok(None);
        }

        let Some(value) = self.values.get(self.cursor) else {
            return Err(ParquetError::General(format!(
                "export column {} row {row} is marked present, but its batch holds only {} value(s)",
                self.leaf,
                self.values.len()
            ))
            .into());
        };

        Ok(Some(value))
    }

    /// Consume the value at `row`, having read it or decided not to.
    ///
    /// Advances only past a **present** row, matching [`take`](Self::take): the cursor runs over the
    /// non-null values, so a null row occupies a definition level and no value slot. Calling this
    /// without a preceding [`peek`](Self::peek) is sound but pointless — it is how a column is
    /// skipped, which no layout currently does.
    fn advance(&mut self, row: usize) {
        if !self.is_null(row) {
            self.cursor += 1;
        }
    }

    /// Whether `row` of the buffered batch is a genuine SQL null.
    fn is_null(&self, row: usize) -> bool {
        self.optional && self.def_levels.get(row).copied().unwrap_or_default() == 0
    }
}

/// The column readers and buffers for the row group currently being decoded.
struct RowGroupCursor {
    ts: BatchedColumn<Int64Type>,
    symbol: BatchedColumn<ByteArrayType>,
    /// One per value column the layout names, in slot order.
    values: Vec<BatchedColumn<DoubleType>>,
    /// Rows in the batch currently buffered.
    rows: usize,
    /// Next row of that batch to decode.
    row: usize,
}

impl RowGroupCursor {
    /// Open every column the plan names in row group `group`.
    ///
    /// # Errors
    /// [`LseError::Parquet`] if the row group or any of its column chunks cannot be opened.
    fn open(
        reader: &SerializedFileReader<File>,
        group: usize,
        plan: &ColumnPlan,
    ) -> Result<Self, LseError> {
        // The column readers own a refcounted handle to the file rather than borrowing this row group,
        // so they outlive it and the decoder needs to keep only them.
        let row_group = reader.get_row_group(group)?;
        let row_group = row_group.as_ref();

        Ok(Self {
            ts: BatchedColumn::open(row_group, plan.ts)?,
            symbol: BatchedColumn::open(row_group, plan.symbol)?,
            values: plan
                .values
                .iter()
                .map(|column| BatchedColumn::open(row_group, *column))
                .collect::<Result<_, _>>()?,
            rows: 0,
            row: 0,
        })
    }

    /// Read the next batch from every column, returning how many rows it holds.
    ///
    /// Zero means the row group is exhausted.
    ///
    /// # Errors
    /// [`LseError::Parquet`] for a decode failure, or if the columns disagree on the row count.
    /// Parquet guarantees they do not; a decoder that read one column short would silently misalign
    /// every later value against its timestamp rather than fail.
    fn read_batch(&mut self) -> Result<usize, LseError> {
        let rows = self.ts.read_batch()?;

        // Non-short-circuiting on purpose: every column must be advanced by the same batch even when
        // one has already disagreed, or the readers desynchronise and the error below is the last
        // truthful thing this cursor could say.
        let mut agreed = self.symbol.read_batch()? == rows;
        for column in &mut self.values {
            agreed &= column.read_batch()? == rows;
        }

        if !agreed {
            return Err(ParquetError::General(format!(
                "export columns disagree on the row count of row group: {COL_TS} yielded {rows} rows"
            ))
            .into());
        }

        self.rows = rows;
        self.row = 0;

        Ok(rows)
    }
}

/// List the distinct symbols an artifact contains, without decoding it.
///
/// Lets a caller validate a file against their instrument registry *before* a backtest. It is
/// deliberately **not** run implicitly: a streaming backtest source is already re-read once per
/// run, and an automatic pre-scan would double that.
///
/// Cheap in practice — only the `symbol` column chunk is read, and it is dictionary-encoded — but it
/// still walks it, so it is opt-in.
///
/// Deliberately does **not** resolve the artifact's layout: a file whose value columns match none of
/// the measured layouts can still answer "which symbols is this?", which is exactly the question worth
/// asking about a file that turned out to be something other than expected.
///
/// Blocking, like [`read_export`]: run it under [`tokio::task::spawn_blocking`] when something else
/// shares the runtime.
///
/// # Errors
/// See [`LseError::Parquet`], [`LseError::UnsupportedSchema`], [`LseError::UnsupportedColumnType`]
/// and [`LseError::NullValue`].
pub fn symbols_in_export(path: impl AsRef<Path>) -> Result<Vec<String>, LseError> {
    let reader = open_reader(path.as_ref())?;
    let fields = reader.metadata().file_metadata().schema().get_fields();

    // Leaf order equals field order only for a flat schema, and this reads by leaf index too.
    if !fields.iter().all(|field| is_flat_primitive(field)) {
        return Err(LseError::UnsupportedSchema {
            columns: column_names(fields),
        });
    }
    let column = utf8_column(fields, COL_SYMBOL)?;

    let mut symbols: Vec<String> = Vec::new();
    for group in 0..reader.num_row_groups() {
        let row_group = reader.get_row_group(group)?;
        let mut buffered = BatchedColumn::<ByteArrayType>::open(row_group.as_ref(), column)?;

        loop {
            let rows = buffered.read_batch()?;
            if rows == 0 {
                break;
            }

            for row in 0..rows {
                // `peek` rather than `take`: `ByteArray` wraps `Option<Bytes>`, so a dictionary-
                // backed clone is an atomic increment and its drop a decrement — two lock-prefixed
                // RMWs per row, for a value only compared and dropped. NLL ends the borrow at the
                // last use of `value`, which is why `advance` can follow.
                let value = buffered
                    .peek(row)?
                    .ok_or(LseError::NullValue { column: COL_SYMBOL })?
                    .as_utf8()?;
                // Compare before allocating: every artifact is single-symbol, so all but the first
                // row of a file that can run to hundreds of thousands would otherwise allocate only
                // to be discarded.
                if !symbols.iter().any(|symbol| symbol == value) {
                    symbols.push(value.to_owned());
                }
                buffered.advance(row);
            }
        }
    }

    Ok(symbols)
}

/// Decode an export artifact into market events for one instrument.
///
/// The returned iterator yields [`MarketEvent`]s in file order, which is ascending by
/// `time_exchange`.
///
/// # ⚠️ This decodes synchronously and can block for a long time
/// Both the file read and the Parquet decode are blocking, over artifacts that run to hundreds of
/// thousands of rows. Called directly from an async task, it stalls that runtime worker for the
/// whole decode. Drive it from
/// [`stream_blocking_iter`](crate::streams::blocking::stream_blocking_iter), which moves the decode
/// onto a blocking thread **and** bounds how far it may run ahead of whatever polls it — that being
/// the merge, not the engine, so it is not an end-to-end memory bound. See below.
///
/// # Combining artifacts
/// Every artifact is single-symbol, so a multi-instrument run merges several with
/// [`merge_time_sorted`](crate::streams::merge::merge_time_sorted). That merge takes *streams* of
/// [`MarketStreamEvent`](crate::streams::consumer::MarketStreamEvent), so a decoded artifact needs
/// two adapting steps — and only two, because these events are already instrument-tagged, unlike
/// the candle replay path's, which additionally need `tag_events`:
///
/// ```no_run
/// # use futures::StreamExt;
/// # use rustrade_data::exchange::lse::export::LseExport;
/// # use rustrade_data::exchange::lse::parquet::read_export;
/// # use rustrade_data::streams::blocking::{stream_blocking_iter, DEFAULT_BLOCKING_CHANNEL_CAPACITY};
/// # use rustrade_data::streams::consumer::MarketStreamEvent;
/// # use rustrade_data::streams::merge::merge_time_sorted;
/// # use rustrade_instrument::instrument::InstrumentIndex;
/// # fn merge(artifacts: Vec<(LseExport, InstrumentIndex)>) {
/// let streams = artifacts.into_iter().map(|(export, instrument)| {
///     // 1. blocking iterator -> bounded stream, off the runtime's workers.
///     stream_blocking_iter(DEFAULT_BLOCKING_CHANNEL_CAPACITY, move || {
///         read_export(&export, instrument)
///     })
///     // 2. bare event -> the merge's reconnect-aware item.
///     .map(|event| event.map(MarketStreamEvent::Item))
/// });
///
/// let _merged = merge_time_sorted(streams);
/// # }
/// ```
///
/// Wrapping the iterator in [`futures::stream::iter`] instead compiles and is lazy, but a blocking
/// iterator never returns `Poll::Pending`, so nothing downstream can pace it: a consumer forwarding
/// into an unbounded channel — the engine feed is one — takes the **whole artifact** into memory
/// before the first event is processed. Laziness does not bound memory on its own; the producer has
/// to be made to wait. One trade-off comes with the bridge: a failure to *open* the artifact arrives
/// as the stream's first `Err` item rather than from this call, so every failure is handled on one
/// path instead of open errors being detected eagerly.
///
/// # `time_exchange` for a candle is the bar's `close_time`
/// The artifact's `ts` column is the bar's **open** instant. A bar enters the timeline when its
/// period *ends*, so `time_exchange` is the derived exclusive close — stamping the open would let a
/// strategy act on a completed bar at the instant its period began, which is lookahead and silent.
/// This matches the candle replay path, so the two producers are interchangeable in a merge. Tick
/// layouts have no such derivation: their `time_exchange` is the observation instant as published.
///
/// # What is verified, and why here
/// - **The schema**, up front, against the columns present — not assumed from the dataset.
/// - **The symbol on every row**, against [`LseExport::symbol`]. Cheap next to Parquet decode, and
///   the only thing that catches a file described by the wrong descriptor.
/// - **Ascending `time_exchange`**, because `MarketDataStreamed` explicitly delegates that
///   obligation to its source rather than checking it. How strictly depends on the layout: a tick
///   or quote tape is checked as **non-decreasing**, since 68% of adjacent rows tie on an equity
///   tape and requiring strict ascent would reject valid files; a candle artifact at a fixed
///   resolution is checked as **strictly ascending**, since two bars of one series cannot share an
///   open and therefore cannot share the close derived from it.
/// - **Bar spacing against the declared resolution**, on a candle artifact at a fixed resolution.
///   Consecutive bars of a compliant series are exactly one interval apart and a gap only ever
///   makes that spacing *wider*, so spacing narrower than the declared interval means the file is
///   at a finer resolution than the caller said — [`LseError::UnexpectedCandleResolution`], the same
///   detector the vault path runs. It is the only thing *in this decoder* that can see that: an
///   artifact records no resolution of its own, and the ordering check above is blind to it because
///   a fixed interval is a constant shift from open to close and cancels out of every comparison it
///   makes. An [`LseExport`] built by `download_export` is additionally cross-checked against the
///   timeframe the job echoed back, but only when it echoed one at all.
///
/// # ⚠️ The `interval` check is one-directional
/// It catches a file **finer** than declared — 1-minute bars declared `Day1`, which would otherwise
/// yield strictly ascending closes with every bar claiming a 24-hour period overlapping the next
/// 1,439.
///
/// It does **not** catch a file **coarser** than declared, and that is the worse of the two
/// mistakes: daily bars declared `Min1` are spaced 24 hours apart, which is wider than the declared
/// 60 seconds and indistinguishable from a genuine gap, so they decode cleanly — and each bar then
/// enters the timeline a minute after its day opened rather than at the end of it, which is a full
/// day of lookahead. The only evidence available is that *no* pair in the file is exactly one
/// interval apart, which a streaming decoder cannot act on: it is a property of the whole artifact,
/// unknown until every event has already been yielded, and a sparse but valid file can legitimately
/// have no such pair. Nor does the check fire on a single-row artifact, which has no pair to
/// measure, or on a **calendar** interval (`Month1`), which has no single width — February would
/// false-positive against a 31-day one.
///
/// Nothing in an artifact records its own resolution, so within those limits only the caller can
/// get this right — prefer [`LseExport`] built from a job whose `timeframe` is known, which
/// `verify_job_covers_request` cross-checks.
///
/// # ⚠️ `instrument` is taken on trust — resolve it from the registry
/// Nothing here can check it: [`InstrumentIndex`] is an unbounded `usize` read positionally by
/// engine state, so a fabricated one attributes this file's prices to a different instrument or
/// panics, and neither the artifact nor this decoder can tell. Obtain it from
/// [`market::instrument_index_for`](super::market::instrument_index_for), which derives it from
/// the registry the engine was built with and additionally rejects the quote-asset disagreement
/// that would book a `.L` listing's pence as pounds — 100× wrong, silently. Passing a hand-built
/// index bypasses both checks.
///
/// # Errors
/// See [`LseError::UnsupportedSchema`], [`LseError::SymbolMismatch`],
/// [`LseError::NonMonotonicTimestamps`], [`LseError::UnexpectedCandleResolution`],
/// [`LseError::PriceNotRepresentable`], [`LseError::TimestampNotRepresentable`],
/// [`LseError::TimestampOverflow`] and [`LseError::Parquet`]. Decode errors are surfaced, never
/// skipped, and the **first one ends the iterator** — see [`LseExportEvents`].
pub fn read_export(
    export: &LseExport,
    instrument: InstrumentIndex,
) -> Result<LseExportEvents, LseError> {
    let reader = open_reader(export.path())?;
    let plan = ColumnPlan::resolve(
        reader.metadata().file_metadata().schema().get_fields(),
        export.timeframe(),
    )?;

    let rows = reader.metadata().file_metadata().num_rows();
    info!(
        path = %export.path().display(),
        dataset = export.dataset().as_catalog_str(),
        symbol = export.symbol(),
        rows,
        layout = ?plan.layout,
        "decoding export artifact"
    );

    if rows == 0 {
        // A zero-row artifact is well-formed and carries the full schema, so nothing downstream
        // would complain. It is the measured result of an `"all"` export, and of a range that
        // matched nothing.
        warn!(
            path = %export.path().display(),
            symbol = export.symbol(),
            "export artifact contains no rows; a backtest fed from it will see no events"
        );
    }

    Ok(LseExportEvents {
        // Owns the reader, so the iterator is `'static` and can outlive this call.
        reader,
        plan,
        cursor: None,
        group: 0,
        expected_symbol: export.symbol().to_owned(),
        exchange: export.exchange_id(),
        instrument,
        previous: None,
        previous_open: None,
        failed: false,
        // Saturating rather than failing: the row count only feeds `size_hint`, which is a hint.
        remaining: usize::try_from(rows).unwrap_or(usize::MAX),
        warned_discarded_volume: false,
    })
}

/// Iterator over the market events decoded from one export artifact.
///
/// Created by [`read_export`].
///
/// # It ends at the first error
/// Any [`Err`] is terminal: the iterator yields it once and every later call returns [`None`]
/// ([`FusedIterator`](std::iter::FusedIterator)). The symbol and ordering checks are **whole-file**
/// invariants rather than per-row conditions, so once one is violated the artifact is known not to
/// hold the property the rest of the pipeline assumes without re-checking, and no later row can
/// restore it. Continuing would hand a caller who discards errors — `.filter_map(Result::ok)`, or a
/// log-and-continue loop — a self-consistent but silently truncated view of a file already proven
/// corrupt, which is the failure mode these checks exist to prevent.
///
/// This deliberately differs from the `databento` DBN iterators, which continue past a per-record
/// decode failure. Their errors are per-record and carry no cross-row meaning; these are verdicts
/// on the file. (Not linked: that module is behind its own feature, so the link would be dead in a
/// build without it.)
pub struct LseExportEvents {
    /// Owns the file, so the iterator is `'static` and the column readers below outlive the row
    /// group they were opened from.
    reader: SerializedFileReader<File>,
    plan: ColumnPlan,
    /// Column readers and buffers for the row group being decoded; `None` before the first and
    /// between each pair.
    cursor: Option<RowGroupCursor>,
    /// The next row group to open.
    group: usize,
    expected_symbol: String,
    exchange: ExchangeId,
    instrument: InstrumentIndex,
    /// The last `time_exchange` yielded — the high-water mark the ordering rule is checked against.
    previous: Option<DateTime<Utc>>,
    /// The last row's `ts` column, which for a candle layout is the bar's **open**.
    ///
    /// Distinct from `previous` above, which for a candle holds the *derived close*. The two track
    /// different invariants: `previous` guards the timeline's order, this guards the artifact's bar
    /// **spacing** against the declared resolution. Spacing is compared on opens rather than closes
    /// — the two are equal for a fixed step, but only the opens are values a caller can find in
    /// their own file, so only the opens are worth naming in the error. On a tick or quote layout
    /// this simply mirrors `previous` and is never read.
    previous_open: Option<DateTime<Utc>>,
    /// Set once any error has been yielded; see the type's documentation.
    ///
    /// Also what makes [`FusedIterator`](std::iter::FusedIterator) sound: a failing row leaves the
    /// column cursors mid-batch and mutually misaligned, so resuming from them would decode values
    /// against the wrong timestamps rather than fail again.
    failed: bool,
    /// Rows not yet consumed, from the file metadata — the `size_hint` upper bound.
    remaining: usize,
    /// One-shot latch for the discarded-`volume` warning; see [`RowLayout::QuoteFromPrice`].
    ///
    /// Per artifact, not per row: the condition is a property of the file, and a tick export runs
    /// to millions of rows.
    warned_discarded_volume: bool,
}

impl std::fmt::Debug for LseExportEvents {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Neither the file reader nor the column readers are `Debug`, and the decoding position is
        // the useful part anyway.
        f.debug_struct("LseExportEvents")
            .field("symbol", &self.expected_symbol)
            .field("exchange", &self.exchange)
            .field("layout", &self.plan.layout)
            .field("previous", &self.previous)
            .finish_non_exhaustive()
    }
}

impl Iterator for LseExportEvents {
    type Item = Result<MarketEvent<InstrumentIndex, DataKind>, LseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }

        match self.read_next() {
            Ok(Some(event)) => Some(Ok(event)),
            Ok(None) => None,
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }

    /// Upper bound only: one event per row, but an error can end the iterator early, so the lower
    /// bound stays zero. The row count comes from the file metadata, read once at construction.
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.failed {
            (0, Some(0))
        } else {
            (0, Some(self.remaining))
        }
    }
}

// Upheld by the `failed` latch: once `next` has returned `None` it can only return `None`, whether
// the file ran out or an error ended it.
impl std::iter::FusedIterator for LseExportEvents {}

impl LseExportEvents {
    /// Read the next row, advancing through batches and row groups as they run out.
    ///
    /// `Ok(None)` means the artifact is exhausted.
    fn read_next(&mut self) -> Result<Option<MarketEvent<InstrumentIndex, DataKind>>, LseError> {
        // The row's values are extracted before decoding rather than decoded in place: `decode`
        // needs `&mut self` for the monotonicity state, which cannot be taken while the cursor is
        // borrowed out of `self`.
        let (micros, values) = loop {
            let Some(cursor) = self.cursor.as_mut() else {
                if self.group >= self.reader.num_row_groups() {
                    return Ok(None);
                }

                // Every measured artifact holds one row group, but that is a property of the
                // provider's writer rather than anything the format promises, so this loops.
                self.check_discarded_volume(self.group);
                self.cursor = Some(RowGroupCursor::open(&self.reader, self.group, &self.plan)?);
                self.group += 1;
                continue;
            };

            if cursor.row == cursor.rows && cursor.read_batch()? == 0 {
                self.cursor = None;
                continue;
            }

            let row = cursor.row;
            cursor.row += 1;

            // Checked before anything else is read: it is the only check that catches a file
            // described by the wrong descriptor, and it compares bytes rather than allocating a
            // `String` per row for a column that is one dictionary-encoded value for the whole file.
            //
            // `peek`, not `take`: the value is compared and dropped, and cloning a `ByteArray` is
            // two atomic refcount operations per row rather than the free copy the `f64` columns
            // get. `advance` after the match completes the read.
            //
            // Both error arms return WITHOUT advancing, where the `take` this replaced advanced
            // unconditionally — so on those paths the symbol cursor is left one row behind the
            // other columns. That is sound only because `next` latches `failed` on any error and
            // never calls `read_next` again, so no later row is decoded against the stale cursor.
            // If that latch is ever removed, `advance` must move above the `match`.
            match cursor.symbol.peek(row)?.map(ByteArray::data) {
                Some(symbol) if symbol == self.expected_symbol.as_bytes() => {}
                Some(symbol) => {
                    return Err(LseError::SymbolMismatch {
                        expected: self.expected_symbol.clone(),
                        found: String::from_utf8_lossy(symbol).into_owned(),
                    });
                }
                None => return Err(LseError::NullValue { column: COL_SYMBOL }),
            }
            cursor.symbol.advance(row);

            let Some(micros) = cursor.ts.take(row)? else {
                return Err(LseError::NullValue { column: COL_TS });
            };

            // Zipped rather than indexed: `cursor.values` is as long as the layout's slot count,
            // which `test_no_layout_exceeds_the_value_column_budget` holds at or below
            // `MAX_VALUE_COLUMNS`, so nothing is dropped and there is no unreachable branch here.
            let mut values = [None; MAX_VALUE_COLUMNS];
            for (slot, column) in values.iter_mut().zip(cursor.values.iter_mut()) {
                *slot = column.take(row)?;
            }

            break (micros, values);
        };

        self.remaining = self.remaining.saturating_sub(1);

        self.decode(micros, &values).map(Some)
    }

    /// Warn, at most once per artifact, if the `volume` column a
    /// [`QuoteFromPrice`](RowLayout::QuoteFromPrice) layout discards has stopped being the measured
    /// constant zero.
    ///
    /// Answered from the row group's **column-chunk statistics**, which the writer already computed
    /// and stored in the footer: a chunk whose `min` and `max` are both zero cannot contain a
    /// non-zero value, so the whole column is cleared without reading a page of it. The previous
    /// version decoded the column alongside the real ones and compared every row — one extra
    /// `DOUBLE` chunk decompressed end to end, roughly 400 MB on a 50M-row artifact, for at most one
    /// log line.
    ///
    /// # When statistics are absent
    /// Nothing is reported for that row group. Writers are not required to emit statistics, and the
    /// alternative — falling back to decoding the column — would reinstate exactly the cost this
    /// avoids, on the artifacts least able to afford it. The check is an observability aid for a
    /// column the layout discards either way, so failing to run it costs no correctness: the decoded
    /// bid/ask are unaffected, which is the same reason a positive result warns rather than errors.
    ///
    /// The `warn` is deliberately not "a non-zero size": what it detects is "not the measured
    /// `0.0`", and mislabelling that as a quantity sends a reader looking for the wrong thing.
    ///
    /// # NaN is not detected
    /// Parquet writers exclude NaN from `min`/`max` by spec, so a chunk of `[0.0, NaN]` reports
    /// bounds of `0.0`/`0.0` and an all-NaN chunk reports no bounds at all — neither warns. The
    /// per-row check this replaced did catch NaN, since `value != 0.0` is true of it. That loss is
    /// accepted for the same reason the absent-statistics case is: this is an observability aid for
    /// a column the layout discards either way, and reinstating the per-row decode to catch it
    /// would cost the whole column on every artifact.
    fn check_discarded_volume(&mut self, group: usize) {
        if self.warned_discarded_volume {
            return;
        }
        let Some(column) = self.plan.discarded_volume else {
            return;
        };

        let Some(statistics) = self
            .reader
            .metadata()
            .row_group(group)
            .column(column.leaf)
            .statistics()
        else {
            return;
        };

        // `min`/`max` are `Option`s of their own: a chunk can carry a null count and no bounds.
        // Absent bounds prove nothing, so they are treated as "not checked" rather than as zero.
        let Statistics::Double(bounds) = statistics else {
            return;
        };
        let populated = matches!(bounds.min_opt(), Some(min) if *min != 0.0)
            || matches!(bounds.max_opt(), Some(max) if *max != 0.0);

        if populated {
            self.warned_discarded_volume = true;
            warn!(
                symbol = %self.expected_symbol,
                "LSE quote export populated the `volume` column with a value other than the \
                 measured `0.0`, which this layout discards: an L1 quote has no undifferentiated \
                 size field. Reported once per artifact; the decoded bid/ask are unaffected"
            );
        }
    }

    /// Decode one row's values, enforcing the ordering and spacing invariants.
    fn decode(
        &mut self,
        micros: i64,
        values: &[Option<f64>; MAX_VALUE_COLUMNS],
    ) -> Result<MarketEvent<InstrumentIndex, DataKind>, LseError> {
        let observed = DateTime::from_timestamp_micros(micros)
            .ok_or(LseError::TimestampNotRepresentable { micros })?;

        // For a candle this is the bar's OPEN instant, so the payload decides the event time.
        let (time_exchange, kind) = self.decode_kind(values, observed)?;

        // The resolution this artifact was declared at, if it is a candle artifact at a FIXED one.
        // Both checks below key on it, and both are therefore skipped for a CALENDAR step, because
        // month arithmetic clamps day-of-month: `2024-01-30 + 1mo` and `2024-01-31 + 1mo` both land
        // on `2024-02-29`, so two legitimate bars share a close, and `month` has no single width to
        // compare a spacing against — February would false-positive against a 31-day one.
        // `historical.rs` skips calendar steps in its own resolution check for the same reasons.
        let fixed_candle = match self.plan.layout {
            RowLayout::Candle { interval, .. } => match interval.to_step() {
                IntervalStep::Fixed(width) => Some((interval, width)),
                IntervalStep::Months(_) => None,
            },
            _ => None,
        };

        // ORDER. The rule is LAYOUT-DEPENDENT, because "two rows share an instant" means different
        // things on the two shapes.
        //
        // On a TICK or QUOTE tape a tie is the common case rather than an edge case — 68% of
        // adjacent rows tie on an equity tape, since several prints can share a microsecond — so
        // only a step backwards is a violation.
        //
        // On a CANDLE artifact at a FIXED resolution a tie is impossible: two bars of one series
        // cannot share an open, so they cannot share the close derived from it. A duplicate there
        // means the file holds more than one series, or repeats a row.
        let violated = self.previous.filter(|previous| {
            if fixed_candle.is_some() {
                time_exchange <= *previous
            } else {
                time_exchange < *previous
            }
        });
        if let Some(previous) = violated {
            return Err(LseError::NonMonotonicTimestamps {
                previous,
                found: time_exchange,
            });
        }

        // RESOLUTION. Consecutive bars of a compliant fixed-interval series are exactly one
        // interval apart, and a gap (a weekend, an exchange holiday, a quiet symbol) only ever
        // makes that spacing WIDER — so spacing NARROWER than the declared interval means the
        // artifact is at a finer resolution than the caller declared, and every `close_time`
        // derived above is wrong.
        //
        // Nothing else here can see that. An artifact records no resolution of its own, the bars
        // still ascend, and the ORDER check above is blind to it by construction: a fixed interval
        // is a constant shift from open to close, so it cancels out of every comparison made there
        // and a mis-declared one still yields distinct, strictly ascending closes. Spacing is the
        // one property that distinguishes it — the same detector `historical.rs` runs against the
        // vault's habit of answering a misspelled resolution parameter with 1-minute bars.
        //
        // Compared on OPENS, which is what `ts` carries. The two spacings are equal for a fixed
        // step, so the choice is purely about what the error names: an open is a value the caller
        // can find in their own file, where the close is one this decoder invented.
        //
        // Only the finer-than-declared direction is reachable; see `read_export`.
        if let (Some((interval, width)), Some(previous_open)) = (fixed_candle, self.previous_open) {
            let actual = observed - previous_open;
            if actual < width {
                return Err(LseError::UnexpectedCandleResolution {
                    symbol: self.expected_symbol.clone(),
                    interval,
                    previous_open,
                    open: observed,
                    actual,
                });
            }
        }

        self.previous = Some(time_exchange);
        self.previous_open = Some(observed);

        Ok(MarketEvent {
            time_exchange,
            // A file replay has no genuine receipt instant; only `time_exchange` orders the
            // timeline, so mirroring it beats a synthetic `Utc::now()`.
            time_received: time_exchange,
            exchange: self.exchange,
            instrument: self.instrument,
            kind,
        })
    }

    /// Build the payload for one row according to the resolved layout.
    ///
    /// Returns the instant the event enters the timeline alongside the payload. For a tick that is
    /// the observation instant; for a candle it is the bar's **close**, not the `ts` it was read
    /// from — see [`read_export`].
    fn decode_kind(
        &mut self,
        values: &[Option<f64>; MAX_VALUE_COLUMNS],
        time: DateTime<Utc>,
    ) -> Result<(DateTime<Utc>, DataKind), LseError> {
        match self.plan.layout {
            RowLayout::Quote { bid, ask } => Ok((
                time,
                quote(
                    time,
                    required(values, bid, "bid")?,
                    required(values, ask, "ask")?,
                ),
            )),
            RowLayout::QuoteFromPrice { price, ask } => {
                // `price` is the bid; `ask` is nullable on these datasets, so a null row yields a
                // one-sided book rather than a fabricated ask. The `volume` column these artifacts
                // also carry is not read here at all — see `check_discarded_volume`.
                let bid = required(values, price, "price")?;
                let kind = match optional(values, ask)? {
                    Some(ask) => quote(time, bid, ask),
                    None => DataKind::OrderBookL1(OrderBookL1 {
                        last_update_time: time,
                        best_bid: Some(Level::new(bid, Decimal::ZERO)),
                        best_ask: None,
                    }),
                };

                Ok((time, kind))
            }
            RowLayout::Trade { price, volume } => Ok((
                time,
                DataKind::Trade(PublicTrade {
                    // The tape carries no trade identifier; the timestamp is not unique (ties are
                    // the norm), so inventing one from it would be a fabricated, colliding id.
                    id: SmolStr::default(),
                    price: required(values, price, "price")?,
                    amount: required(values, volume, "volume")?,
                    // No aggressor side is published, and it is not inferable from a bid-side price.
                    side: None,
                }),
            )),
            RowLayout::Candle {
                open,
                high,
                low,
                close,
                volume,
                interval,
            } => {
                // `ts` is the bar's OPEN time; the library's contract is an exclusive `close_time`.
                let close_time = close_time_from_open(time, interval.to_step()).ok_or(
                    LseError::TimestampOverflow {
                        open: time,
                        interval,
                    },
                )?;

                Ok((
                    // A bar enters the timeline when its period ENDS, matching the candle replay
                    // path. Stamping the open would let a strategy act on a completed bar at the
                    // instant its period began — silent lookahead.
                    close_time,
                    DataKind::Candle(Candle {
                        close_time,
                        open: required(values, open, "open")?,
                        high: required(values, high, "high")?,
                        low: required(values, low, "low")?,
                        close: required(values, close, "close")?,
                        // Absent column => `None` (FX). Present => faithful pass-through, including
                        // a literal zero: rewriting the provider's number would be inventing a fact.
                        volume: match volume {
                            Some(volume) => optional(values, volume)?,
                            None => None,
                        },
                        // Never published on any dataset.
                        trade_count: None,
                    }),
                ))
            }
        }
    }
}

/// Build a two-sided L1 book.
///
/// Both levels carry a zero size: the export publishes prices only, and a fabricated size would be
/// indistinguishable from a real one downstream.
fn quote(time: DateTime<Utc>, bid: Decimal, ask: Decimal) -> DataKind {
    DataKind::OrderBookL1(OrderBookL1 {
        last_update_time: time,
        best_bid: Some(Level::new(bid, Decimal::ZERO)),
        best_ask: Some(Level::new(ask, Decimal::ZERO)),
    })
}

/// Read a layout slot the payload has no substitute for, as a [`Decimal`].
///
/// `column` names the slot for the diagnostic; it is passed at the call site because the slot is an
/// index into the row's values rather than something that carries its own name.
fn required(
    values: &[Option<f64>; MAX_VALUE_COLUMNS],
    slot: usize,
    column: &'static str,
) -> Result<Decimal, LseError> {
    values
        .get(slot)
        .copied()
        .flatten()
        .ok_or(LseError::NullValue { column })
        .and_then(convert)
}

/// Read a layout slot that may legitimately be null, mapping SQL null to `None`.
fn optional(
    values: &[Option<f64>; MAX_VALUE_COLUMNS],
    slot: usize,
) -> Result<Option<Decimal>, LseError> {
    values.get(slot).copied().flatten().map(convert).transpose()
}

/// Convert a provider `f64` into a [`Decimal`], surfacing rather than swallowing a failure.
///
/// `f64::NAN` and the infinities have no [`Decimal`] representation. Substituting zero would put a
/// real-looking price into the money path.
fn convert(value: f64) -> Result<Decimal, LseError> {
    Decimal::try_from(value).map_err(|error| LseError::PriceNotRepresentable {
        value,
        message: error.to_string(),
    })
}

/// Open an artifact for reading.
fn open_reader(path: &Path) -> Result<SerializedFileReader<File>, LseError> {
    let file = File::open(path).map_err(|source| LseError::Io {
        message: format!("opening {}", path.display()),
        source,
    })?;

    Ok(SerializedFileReader::new(file)?)
}

#[cfg(test)]
// Test code: a hand-built schema that fails to build is a bug in the test itself, not a runtime
// condition this crate's error policy applies to.
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn field(name: &str, physical: PhysicalType, logical: Option<LogicalType>) -> TypePtr {
        Arc::new(
            SchemaType::primitive_type_builder(name, physical)
                .with_repetition(Repetition::REQUIRED)
                .with_logical_type(logical)
                .build()
                .expect("hand-built test schema field is valid"),
        )
    }

    /// A flat schema carrying `ts`, `symbol` and one `DOUBLE` per named value column.
    fn schema(values: &[&str]) -> Vec<TypePtr> {
        let mut fields = vec![
            field(
                COL_TS,
                PhysicalType::INT64,
                Some(LogicalType::timestamp(true, TimeUnit::MICROS)),
            ),
            field(
                COL_SYMBOL,
                PhysicalType::BYTE_ARRAY,
                Some(LogicalType::String),
            ),
        ];
        fields.extend(
            values
                .iter()
                .map(|name| field(name, PhysicalType::DOUBLE, None)),
        );

        fields
    }

    #[test]
    fn test_no_layout_exceeds_the_value_column_budget() {
        // One case per layout `ColumnPlan::resolve` can produce, each in its widest form. A row's
        // values are extracted into a `MAX_VALUE_COLUMNS`-wide array by zipping it against the
        // plan's columns, so a layout naming more than that would silently drop the extras — and
        // the slot the layout points at would then read whatever happened to land there.
        let cases = [
            (
                schema(&["open", "high", "low", "close", "volume"]),
                LseExportTimeframe::Candle(CandleInterval::Min1),
            ),
            (
                schema(&["open", "high", "low", "close"]),
                LseExportTimeframe::Candle(CandleInterval::Min1),
            ),
            (schema(&["bid", "ask"]), LseExportTimeframe::Tick),
            (
                schema(&["price", "ask", "volume"]),
                LseExportTimeframe::Tick,
            ),
            (schema(&["price", "volume"]), LseExportTimeframe::Tick),
        ];

        for (fields, timeframe) in cases {
            let plan =
                ColumnPlan::resolve(&fields, timeframe).expect("measured layout must resolve");

            assert!(
                plan.values.len() <= MAX_VALUE_COLUMNS,
                "{:?} names {} value columns, over the budget of {MAX_VALUE_COLUMNS}",
                plan.layout,
                plan.values.len()
            );
        }
    }
}
