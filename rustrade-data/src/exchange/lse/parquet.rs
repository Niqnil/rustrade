//! Decode an LSE Data 3.0 Parquet export into market events.
//!
//! Behind the `lse-parquet` feature: the core `lse` feature downloads exports without any Parquet
//! dependency, so a consumer who only wants files on disk pays nothing for this.
//!
//! # What decodes, and into what
//! The caller says what a file holds with the [`LseExportData`] it was exported as; a file records
//! nothing about itself.
//!
//! ```text
//! Candles(interval)  time, open, high, low, close, volume, trades [, date]  => DataKind::Candle
//! TradeSlice         time, price, size, kind                                => DataKind::Trade
//! Greeks             not decoded here: UnsupportedExportData
//! ```
//!
//! Greeks are left to [`fetch_option_greeks`](super::client::LseClient::fetch_option_greeks): their
//! figures are stored as 32-bit floats, and widening one to the 64-bit figure an
//! [`OptionGreeks`](crate::subscription::greeks::OptionGreeks) holds adds digits the provider never
//! computed, where the CSV text carries the shortest exact spelling.
//!
//! A candle is checked and converted by the same rules as on the CSV path, so a bar has the same
//! `close_time` whichever format it arrived in. Prices are converted to [`Decimal`] through the
//! shortest decimal text that reads back as the same `f64`, which is how the provider spells a
//! number in CSV.
//!
//! # ⚠️ Nothing in a file names its instrument
//! A file carries no symbol column and no metadata, so nothing here can check that a file belongs
//! to the instrument it is decoded for. A file of `BP` decoded as `BP.L` would yield prices in the
//! wrong currency, unremarked. Keep the file name
//! [`LseExportRequest::file_name`](super::export::LseExportRequest::file_name) gives, which records
//! the selection.
//!
//! # ⚠️ Licensing
//! Decoded data is **not redistributable**. See the [module documentation](super#licensing) and
//! <https://londonstrategicedge.com/licence>.

use crate::event::{DataKind, MarketEvent};
use crate::exchange::lse::error::LseError;
use crate::exchange::lse::export::LseExportData;
use crate::exchange::lse::historical::{CandleRow, CandleSeries};
use crate::subscription::candle::CandleInterval;
use crate::subscription::trade::PublicTrade;
use chrono::{DateTime, TimeDelta, Utc};
use parquet::basic::{ConvertedType, LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use parquet::column::reader::ColumnReaderImpl;
use parquet::data_type::{ByteArrayType, DataType, DoubleType, Int32Type, Int64Type};
use parquet::errors::ParquetError;
use parquet::file::reader::{FileReader, RowGroupReader, SerializedFileReader};
use parquet::schema::types::{Type as SchemaType, TypePtr};
use rust_decimal::Decimal;
use rustrade_instrument::{exchange::ExchangeId, instrument::InstrumentIndex};
use smol_str::SmolStr;
use std::fmt::Write as _;
use std::fs::File;
use std::path::Path;
use std::str::FromStr;
use tracing::{info, warn};

/// Rows decoded per column-reader call.
///
/// Bounds the decoder's working set independently of how the file was written: the buffers are
/// this wide, not one row group wide (the provider writes groups of about 100,000 rows). Large
/// enough to amortise the per-call overhead, small enough that every buffer together stays well
/// inside a megabyte.
const BATCH_ROWS: usize = 8 * 1024;

/// The only `kind` a trade-slice row is decoded from.
const TRADE_KIND: &str = "trade";

/// The unit of a timestamp column, read from the schema.
///
/// Read rather than assumed: the provider writes candles in milliseconds and trade slices in
/// nanoseconds, and a column read in the wrong unit shifts every event by a factor of a thousand or
/// more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimestampUnit {
    Millis,
    Micros,
    Nanos,
}

impl TimestampUnit {
    fn name(self) -> &'static str {
        match self {
            Self::Millis => "milliseconds",
            Self::Micros => "microseconds",
            Self::Nanos => "nanoseconds",
        }
    }

    fn to_time(self, value: i64) -> Result<DateTime<Utc>, LseError> {
        let time = match self {
            Self::Millis => DateTime::from_timestamp_millis(value),
            Self::Micros => DateTime::from_timestamp_micros(value),
            // Every `i64` of nanoseconds is a representable instant.
            Self::Nanos => Some(DateTime::from_timestamp_nanos(value)),
        };
        time.ok_or(LseError::TimestampNotRepresentable {
            value,
            unit: self.name(),
        })
    }
}

/// A leaf column the decoder reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LeafColumn {
    /// Its index among the schema's leaf columns, which in a flat schema is its field position.
    leaf: usize,
    /// Whether it can be null, i.e. whether its chunk carries definition levels.
    optional: bool,
}

/// The columns of a candle file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CandleColumns {
    time: LeafColumn,
    unit: TimestampUnit,
    open: LeafColumn,
    high: LeafColumn,
    low: LeafColumn,
    close: LeafColumn,
    volume: Option<LeafColumn>,
    /// With whether its `INT64` holds unsigned values.
    trades: Option<(LeafColumn, bool)>,
    /// Present for `1d` and longer.
    date: Option<LeafColumn>,
}

/// The columns of a trade-slice file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TradeColumns {
    time: LeafColumn,
    unit: TimestampUnit,
    price: LeafColumn,
    size: LeafColumn,
    kind: LeafColumn,
}

/// Which columns a file is read through, resolved from its schema once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Plan {
    Candles(CandleColumns, CandleInterval),
    Trades(TradeColumns),
}

impl Plan {
    /// Resolve the columns `data` needs from `fields`.
    ///
    /// # Errors
    /// [`LseError::UnsupportedExportData`] for a kind not decoded here,
    /// [`LseError::UnsupportedSchema`] for a missing column or a schema that is not flat, and
    /// [`LseError::UnsupportedColumnType`] for a column of the wrong type.
    fn resolve(fields: &[TypePtr], data: LseExportData) -> Result<Self, LseError> {
        // Columns are read by leaf index, which equals a field's position only in a flat schema.
        if !fields.iter().all(|field| is_flat_primitive(field)) {
            return Err(LseError::UnsupportedSchema {
                columns: column_names(fields),
            });
        }

        match data {
            LseExportData::Candles(interval) => {
                let (time, unit) = timestamp_column(fields, "time")?;
                let sessions = CandleSeries::new("", interval).has_sessions();
                Ok(Self::Candles(
                    CandleColumns {
                        time,
                        unit,
                        open: double_column(fields, "open")?,
                        high: double_column(fields, "high")?,
                        low: double_column(fields, "low")?,
                        close: double_column(fields, "close")?,
                        volume: column(fields, "volume")
                            .map(|_| double_column(fields, "volume"))
                            .transpose()?,
                        trades: column(fields, "trades")
                            .map(|_| count_column(fields, "trades"))
                            .transpose()?,
                        date: sessions.then(|| date_column(fields, "date")).transpose()?,
                    },
                    interval,
                ))
            }
            LseExportData::TradeSlice => {
                let (time, unit) = timestamp_column(fields, "time")?;
                Ok(Self::Trades(TradeColumns {
                    time,
                    unit,
                    price: double_column(fields, "price")?,
                    size: double_column(fields, "size")?,
                    kind: utf8_column(fields, "kind")?,
                }))
            }
            other => Err(LseError::UnsupportedExportData {
                data: other.label().to_owned(),
            }),
        }
    }
}

/// Locate a column by name.
fn column<'a>(fields: &'a [TypePtr], name: &str) -> Option<(usize, &'a TypePtr)> {
    fields
        .iter()
        .enumerate()
        .find(|(_, field)| field.name() == name)
}

/// The schema's column names, for a diagnostic.
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

/// Locate a named column, or report the schema as not the one expected.
fn required_column<'a>(
    fields: &'a [TypePtr],
    name: &str,
) -> Result<(usize, &'a TypePtr), LseError> {
    column(fields, name).ok_or_else(|| LseError::UnsupportedSchema {
        columns: column_names(fields),
    })
}

fn leaf(index: usize, field: &SchemaType) -> LeafColumn {
    LeafColumn {
        leaf: index,
        optional: is_optional(field),
    }
}

fn wrong_type(name: &str, required: &'static str, field: &SchemaType) -> LseError {
    LseError::UnsupportedColumnType {
        column: name.to_owned(),
        required,
        found: describe_type(field),
    }
}

/// Locate `name` and require it to be a `DOUBLE`.
fn double_column(fields: &[TypePtr], name: &str) -> Result<LeafColumn, LseError> {
    let (index, field) = required_column(fields, name)?;
    if field.get_physical_type() != PhysicalType::DOUBLE {
        return Err(wrong_type(name, "DOUBLE", field));
    }
    Ok(leaf(index, field))
}

/// Locate `name` and require it to be a UTF-8 `BYTE_ARRAY`.
fn utf8_column(fields: &[TypePtr], name: &str) -> Result<LeafColumn, LseError> {
    let (index, field) = required_column(fields, name)?;
    let info = field.get_basic_info();
    let utf8 = matches!(info.logical_type_ref(), Some(LogicalType::String))
        || info.converted_type() == ConvertedType::UTF8;
    if field.get_physical_type() != PhysicalType::BYTE_ARRAY || !utf8 {
        return Err(wrong_type(name, "BYTE_ARRAY (STRING)", field));
    }
    Ok(leaf(index, field))
}

/// Locate `name` and require an `INT64` count, returning whether it is declared unsigned.
fn count_column(fields: &[TypePtr], name: &str) -> Result<(LeafColumn, bool), LseError> {
    let (index, field) = required_column(fields, name)?;
    let info = field.get_basic_info();
    if field.get_physical_type() != PhysicalType::INT64 {
        return Err(wrong_type(name, "INT64", field));
    }
    let unsigned = match info.logical_type_ref() {
        Some(LogicalType::Integer(integer)) => !integer.is_signed,
        Some(_) => return Err(wrong_type(name, "INT64", field)),
        None => info.converted_type() == ConvertedType::UINT_64,
    };
    Ok((leaf(index, field), unsigned))
}

/// Locate `name` and require an `INT32` date.
fn date_column(fields: &[TypePtr], name: &str) -> Result<LeafColumn, LseError> {
    let (index, field) = required_column(fields, name)?;
    let info = field.get_basic_info();
    let date = matches!(info.logical_type_ref(), Some(LogicalType::Date))
        || info.converted_type() == ConvertedType::DATE;
    if field.get_physical_type() != PhysicalType::INT32 || !date {
        return Err(wrong_type(name, "INT32 (DATE)", field));
    }
    Ok(leaf(index, field))
}

/// Locate `name` and require a **UTC-adjusted** `INT64` timestamp, returning its unit.
///
/// The UTC flag is the load-bearing half; see [`LseError::UnsupportedColumnType`] for why a
/// local-time column read as epoch time is undetectable downstream.
fn timestamp_column(
    fields: &[TypePtr],
    name: &str,
) -> Result<(LeafColumn, TimestampUnit), LseError> {
    const REQUIRED: &str = "INT64 (TIMESTAMP, is_adjusted_to_utc = true)";
    let (index, field) = required_column(fields, name)?;
    let info = field.get_basic_info();
    if field.get_physical_type() != PhysicalType::INT64 {
        return Err(wrong_type(name, REQUIRED, field));
    }

    let unit = match info.logical_type_ref() {
        Some(LogicalType::Timestamp(timestamp)) if timestamp.is_adjusted_to_u_t_c => {
            match timestamp.unit {
                TimeUnit::MILLIS => TimestampUnit::Millis,
                TimeUnit::MICROS => TimestampUnit::Micros,
                TimeUnit::NANOS => TimestampUnit::Nanos,
            }
        }
        // A writer predating the logical-type union records only the converted type, which the
        // Parquet specification defines as UTC-normalised. `parquet` maps both values of
        // `is_adjusted_to_utc` onto that same converted type, so it is evidence of UTC only when no
        // logical type is present to contradict it.
        None => match info.converted_type() {
            ConvertedType::TIMESTAMP_MILLIS => TimestampUnit::Millis,
            ConvertedType::TIMESTAMP_MICROS => TimestampUnit::Micros,
            _ => return Err(wrong_type(name, REQUIRED, field)),
        },
        Some(_) => return Err(wrong_type(name, REQUIRED, field)),
    };

    Ok((leaf(index, field), unit))
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

/// One leaf column, read a batch of rows at a time into reusable buffers.
///
/// Each column owns two buffers for the whole decode and a batch read appends into them, so a row
/// costs no allocation.
struct BatchedColumn<T: DataType> {
    reader: ColumnReaderImpl<T>,
    /// Index of this leaf in the schema, so an underrun can name the column it happened in.
    leaf: usize,
    /// `true` when the column is `OPTIONAL`, so `values` is sparse and `def_levels` says which rows
    /// it covers.
    optional: bool,
    /// The batch's **non-null** values, in row order. A null row occupies no slot.
    values: Vec<T::T>,
    /// One definition level per row of the batch: `1` present, `0` null. Left empty for a
    /// `REQUIRED` column, which cannot be null.
    def_levels: Vec<i16>,
    /// Index into `values` of the next row's value. Rows are consumed strictly in order, so a
    /// cursor is enough; counting definition levels per row would make the decode quadratic.
    cursor: usize,
}

impl<T: DataType> BatchedColumn<T> {
    /// Open a reader for `column`'s chunk in `row_group`.
    ///
    /// # Errors
    /// [`LseError::Parquet`] if the chunk cannot be opened, or if its physical type is not `T`'s,
    /// which [`Plan::resolve`] has already ruled out; reported rather than raised as the panic
    /// `parquet`'s own `get_typed_column_reader` would.
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

    /// Read up to [`BATCH_ROWS`] more rows, returning how many were read. Zero means the column
    /// chunk is exhausted.
    fn read_batch(&mut self) -> Result<usize, LseError> {
        // `read_records` appends rather than overwrites, so the previous batch has to go first.
        self.values.clear();
        self.def_levels.clear();
        self.cursor = 0;

        // A column with definition levels rejects a call that supplies nowhere to put them, and one
        // without them ignores the buffer, so the two cases cannot share one call.
        let (rows, _values, _levels) = if self.optional {
            self.reader.read_records(
                BATCH_ROWS,
                Some(&mut self.def_levels),
                None,
                &mut self.values,
            )?
        } else {
            self.reader
                .read_records(BATCH_ROWS, None, None, &mut self.values)?
        };

        Ok(rows)
    }

    /// Borrow the value at `row` of the batch, or `None` if that row is null, then move past it.
    ///
    /// Must be called once per row, in row order: the cursor over the non-null values only advances
    /// here.
    ///
    /// # Errors
    /// [`LseError::Parquet`] if the row is marked present but the batch holds no value for it — a
    /// malformed chunk. Reported rather than read as a null, which would blame the file for a value
    /// that is not missing in it.
    fn next_value(&mut self, row: usize) -> Result<Option<&T::T>, LseError> {
        if self.optional && self.def_levels.get(row).copied().unwrap_or_default() == 0 {
            return Ok(None);
        }

        let index = self.cursor;
        self.cursor += 1;
        self.values.get(index).map(Some).ok_or_else(|| {
            ParquetError::General(format!(
                "export column {} row {row} is marked present, but its batch holds only {} \
                 value(s)",
                self.leaf,
                self.values.len()
            ))
            .into()
        })
    }
}

/// A required value, or [`LseError::NullValue`].
fn present<V>(value: Option<V>, column: &'static str) -> Result<V, LseError> {
    value.ok_or(LseError::NullValue { column })
}

/// Converts a provider `f64` to a [`Decimal`] through its shortest round-trip decimal text.
///
/// `f64`'s `Display` prints the shortest decimal that reads back as the same `f64`, the text the
/// provider writes in CSV, so a value decodes to the same [`Decimal`] from either format.
/// `Decimal::try_from(f64)` keeps about fifteen significant digits instead. The buffer is reused,
/// so a conversion allocates nothing once warm.
#[derive(Debug, Default)]
struct DecimalText {
    buffer: String,
}

impl DecimalText {
    fn convert(&mut self, value: f64) -> Result<Decimal, LseError> {
        self.buffer.clear();
        // Writing to a `String` cannot fail.
        let _ = write!(self.buffer, "{value}");
        Decimal::from_str(&self.buffer).map_err(|error| LseError::PriceNotRepresentable {
            value,
            message: error.to_string(),
        })
    }
}

/// The readers and buffers of a candle file's current row group.
struct CandleCursor {
    time: BatchedColumn<Int64Type>,
    open: BatchedColumn<DoubleType>,
    high: BatchedColumn<DoubleType>,
    low: BatchedColumn<DoubleType>,
    close: BatchedColumn<DoubleType>,
    volume: Option<BatchedColumn<DoubleType>>,
    trades: Option<BatchedColumn<Int64Type>>,
    date: Option<BatchedColumn<Int32Type>>,
}

/// The readers and buffers of a trade-slice file's current row group.
struct TradeCursor {
    time: BatchedColumn<Int64Type>,
    price: BatchedColumn<DoubleType>,
    size: BatchedColumn<DoubleType>,
    kind: BatchedColumn<ByteArrayType>,
}

/// One file kind's column readers. Boxed: a candle cursor holds twice the readers of a trade one,
/// and one is allocated per row group, not per row.
enum Columns {
    Candles(Box<CandleCursor>),
    Trades(Box<TradeCursor>),
}

/// The column readers of the row group being decoded, and the position in its current batch.
struct RowGroupCursor {
    columns: Columns,
    /// Rows in the batch currently buffered.
    rows: usize,
    /// Next row of that batch to decode.
    row: usize,
}

impl RowGroupCursor {
    /// Open every column the plan names in row group `group`.
    fn open(
        reader: &SerializedFileReader<File>,
        group: usize,
        plan: &Plan,
    ) -> Result<Self, LseError> {
        // The column readers hold their own handle to the file rather than borrowing this row
        // group, so they outlive it.
        let row_group = reader.get_row_group(group)?;
        let row_group = row_group.as_ref();

        let columns = match plan {
            Plan::Candles(columns, _) => Columns::Candles(Box::new(CandleCursor {
                time: BatchedColumn::open(row_group, columns.time)?,
                open: BatchedColumn::open(row_group, columns.open)?,
                high: BatchedColumn::open(row_group, columns.high)?,
                low: BatchedColumn::open(row_group, columns.low)?,
                close: BatchedColumn::open(row_group, columns.close)?,
                volume: columns
                    .volume
                    .map(|column| BatchedColumn::open(row_group, column))
                    .transpose()?,
                trades: columns
                    .trades
                    .map(|(column, _)| BatchedColumn::open(row_group, column))
                    .transpose()?,
                date: columns
                    .date
                    .map(|column| BatchedColumn::open(row_group, column))
                    .transpose()?,
            })),
            Plan::Trades(columns) => Columns::Trades(Box::new(TradeCursor {
                time: BatchedColumn::open(row_group, columns.time)?,
                price: BatchedColumn::open(row_group, columns.price)?,
                size: BatchedColumn::open(row_group, columns.size)?,
                kind: BatchedColumn::open(row_group, columns.kind)?,
            })),
        };

        Ok(Self {
            columns,
            rows: 0,
            row: 0,
        })
    }

    /// Read the next batch from every column, returning how many rows it holds. Zero means the row
    /// group is exhausted.
    ///
    /// # Errors
    /// [`LseError::Parquet`] for a decode failure, or if the columns disagree on the row count.
    /// Parquet guarantees they do not; a decoder that read one column short would misalign every
    /// later value against its timestamp rather than fail.
    fn read_batch(&mut self) -> Result<usize, LseError> {
        // Every column is advanced by the same batch even after one has disagreed, so the error
        // below is the last thing the cursor says.
        let (rows, agreed) = match &mut self.columns {
            Columns::Candles(c) => {
                let rows = c.time.read_batch()?;
                let mut agreed = c.open.read_batch()? == rows;
                agreed &= c.high.read_batch()? == rows;
                agreed &= c.low.read_batch()? == rows;
                agreed &= c.close.read_batch()? == rows;
                if let Some(column) = &mut c.volume {
                    agreed &= column.read_batch()? == rows;
                }
                if let Some(column) = &mut c.trades {
                    agreed &= column.read_batch()? == rows;
                }
                if let Some(column) = &mut c.date {
                    agreed &= column.read_batch()? == rows;
                }
                (rows, agreed)
            }
            Columns::Trades(c) => {
                let rows = c.time.read_batch()?;
                let mut agreed = c.price.read_batch()? == rows;
                agreed &= c.size.read_batch()? == rows;
                agreed &= c.kind.read_batch()? == rows;
                (rows, agreed)
            }
        };

        if !agreed {
            return Err(ParquetError::General(format!(
                "export columns disagree on the row count of a row group: time yielded {rows} rows"
            ))
            .into());
        }

        self.rows = rows;
        self.row = 0;
        Ok(rows)
    }
}

/// Decode an export file into `(time_exchange, payload)` rows, in file order.
///
/// The lower layer of [`read_export`], for a caller that wants the payloads without an instrument
/// key. `data` must be what the file was exported as: [`LseExportData::Candles`] at its interval,
/// or [`LseExportData::TradeSlice`].
///
/// - A **candle** row is a [`DataKind::Candle`] stamped with its `close_time`: its open plus the
///   interval, as on the CSV path. For `1d` and longer the open is the first trade of the session,
///   week or month, not midnight.
/// - A **trade-slice** row is a [`DataKind::Trade`] stamped with its `time`: a [`PublicTrade`] with
///   an empty `id` (the provider sends no trade identifier) and no `side` (it reports no
///   aggressor), sized in the instrument's units.
///
/// # ⚠️ This decodes synchronously and can block for a long time
/// Both the file read and the decode are blocking, over files of up to 2,500,000 rows. Called
/// directly from an async task, it stalls that runtime worker for the whole decode; see
/// [`read_export`] for how to drive it.
///
/// # What is checked
/// - **The schema**, up front: every column `data` needs, by name and type, the timestamp's unit
///   read from the schema and required to be UTC.
/// - **Candles** by the CSV path's rules: strictly ascending, intraday bars no closer than the
///   interval (so a file finer than declared fails), a session date on every `1d` and longer bar.
///   A file *coarser* than declared cannot be detected: its bars are merely spaced wider, like a
///   gap. Nothing in a file records its resolution, so only the caller can get this right.
/// - **Trades**: `time` non-decreasing (many trades share an instant), every `kind` `trade`.
/// - **No null** in a timestamp, price, size or kind; a null `volume` or `trades` is `None`.
///
/// # Errors
/// [`LseError::UnsupportedExportData`] for Greeks; [`LseError::Io`] if the file cannot be opened;
/// [`LseError::Parquet`], [`LseError::UnsupportedSchema`] and [`LseError::UnsupportedColumnType`]
/// for a file this cannot read. Errors while decoding are yielded, and the first one ends the
/// iterator; see [`LseExportRows`].
pub fn read_export_rows(
    path: impl AsRef<Path>,
    data: LseExportData,
) -> Result<LseExportRows, LseError> {
    let path = path.as_ref();
    let file = File::open(path).map_err(|source| LseError::Io {
        message: format!("opening {}", path.display()),
        source,
    })?;
    let reader = SerializedFileReader::new(file)?;
    let plan = Plan::resolve(
        reader.metadata().file_metadata().schema().get_fields(),
        data,
    )?;

    let rows = reader.metadata().file_metadata().num_rows();
    info!(path = %path.display(), data = data.label(), rows, "decoding lse export");
    if rows == 0 {
        warn!(
            path = %path.display(),
            "lse export holds no rows; a backtest fed from it will see no events"
        );
    }

    let label = path.display().to_string();
    let candles = match plan {
        Plan::Candles(_, interval) => Some(CandleSeries::new(label.clone(), interval)),
        Plan::Trades(_) => None,
    };

    Ok(LseExportRows {
        reader,
        plan,
        label,
        cursor: None,
        group: 0,
        candles,
        previous_trade: None,
        decimal: DecimalText::default(),
        failed: false,
        // Saturating: the row count only feeds `size_hint`.
        remaining: usize::try_from(rows).unwrap_or(usize::MAX),
    })
}

/// Decode an export file into market events for one instrument.
///
/// [`read_export_rows`] with each row wrapped as a [`MarketEvent`] on `exchange` for `instrument`,
/// stamped with the row's time as both `time_exchange` and `time_received` (a file has no receipt
/// instant of its own). See it for what is decoded, how it is stamped and what is checked.
///
/// # Driving it from async code
/// Drive the iterator with
/// [`stream_blocking_iter`](crate::streams::blocking::stream_blocking_iter), which moves the
/// decode onto the blocking pool **and** bounds how far it may run ahead of whatever polls it.
/// Wrapping it in [`futures::stream::iter`] instead compiles and is lazy, but a blocking iterator
/// never returns `Poll::Pending`, so a consumer forwarding into an unbounded channel takes the
/// whole file into memory before the first event is processed.
///
/// Several files, one per instrument, merge into one time-ordered feed with
/// [`merge_time_sorted`](crate::streams::merge::merge_time_sorted):
///
/// ```no_run
/// # use futures::StreamExt;
/// # use rustrade_data::exchange::lse::export::LseExportData;
/// # use rustrade_data::exchange::lse::parquet::read_export;
/// # use rustrade_data::streams::blocking::{stream_blocking_iter, DEFAULT_BLOCKING_CHUNK_SIZE};
/// # use rustrade_data::streams::consumer::MarketStreamEvent;
/// # use rustrade_data::streams::merge::merge_time_sorted;
/// # use rustrade_data::subscription::candle::CandleInterval;
/// # use rustrade_instrument::{exchange::ExchangeId, instrument::InstrumentIndex};
/// # use std::path::PathBuf;
/// # fn merge(files: Vec<(PathBuf, InstrumentIndex)>) {
/// let data = LseExportData::Candles(CandleInterval::Min1);
/// let streams = files.into_iter().map(|(path, instrument)| {
///     // 1. blocking iterator -> bounded stream, off the runtime's workers.
///     stream_blocking_iter(DEFAULT_BLOCKING_CHUNK_SIZE, move || {
///         read_export(&path, data, ExchangeId::LseEquities, instrument)
///     })
///     // 2. bare event -> the merge's reconnect-aware item.
///     .map(|event| event.map(MarketStreamEvent::Item))
/// });
///
/// let _merged = merge_time_sorted(streams);
/// # }
/// ```
///
/// A failure to open the file then arrives as the stream's first `Err` item rather than from this
/// call, so every failure is handled on one path.
///
/// # ⚠️ `exchange` and `instrument` are taken on trust
/// Nothing in the file can check them. Derive them from the registry the engine was built with, as
/// [`LseCandleSource::resolve`](super::backtest::LseCandleSource::resolve) does: it rejects an
/// unknown symbol and a quote-asset disagreement (a `.L` listing registered in pounds rather than
/// pence, 100× wrong), neither of which this can see.
///
/// # Errors
/// As [`read_export_rows`].
pub fn read_export(
    path: impl AsRef<Path>,
    data: LseExportData,
    exchange: ExchangeId,
    instrument: InstrumentIndex,
) -> Result<LseExportEvents, LseError> {
    Ok(LseExportEvents {
        rows: read_export_rows(path, data)?,
        exchange,
        instrument,
    })
}

/// Iterator over the `(time_exchange, payload)` rows of one export file, from
/// [`read_export_rows`].
///
/// # It ends at the first error
/// Any [`Err`] is terminal: it is yielded once and every later call returns [`None`]
/// ([`FusedIterator`](std::iter::FusedIterator)). The ordering and kind checks are verdicts on the
/// whole file, so once one fails no later row can restore the property the rest of a pipeline
/// assumes; continuing would hand a caller who discards errors a silently truncated view of a file
/// already known to be bad. A failing row also leaves the column cursors mid-batch, so resuming
/// would decode values against the wrong timestamps.
pub struct LseExportRows {
    /// Owns the file, so the iterator is `'static` and `Send`.
    reader: SerializedFileReader<File>,
    plan: Plan,
    /// Names the file in errors.
    label: String,
    /// Readers for the row group being decoded; `None` before the first and between each pair.
    cursor: Option<RowGroupCursor>,
    /// The next row group to open.
    group: usize,
    /// The candle checks, for a candle file.
    candles: Option<CandleSeries>,
    /// The last trade's time, for the ordering check.
    previous_trade: Option<DateTime<Utc>>,
    decimal: DecimalText,
    /// Set once any error has been yielded.
    failed: bool,
    /// Rows not yet consumed, from the file metadata: the `size_hint` upper bound.
    remaining: usize,
}

impl std::fmt::Debug for LseExportRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The file and column readers are not `Debug`; the position is the useful part.
        f.debug_struct("LseExportRows")
            .field("file", &self.label)
            .field("plan", &self.plan)
            .field("group", &self.group)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl Iterator for LseExportRows {
    type Item = Result<(DateTime<Utc>, DataKind), LseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }

        match self.read_next() {
            Ok(Some(row)) => {
                self.remaining = self.remaining.saturating_sub(1);
                Some(Ok(row))
            }
            Ok(None) => None,
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }

    /// Upper bound only: an error can end the iterator early.
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.failed {
            (0, Some(0))
        } else {
            (0, Some(self.remaining))
        }
    }
}

// Upheld by the `failed` latch: once `next` has returned `None` it only returns `None`.
impl std::iter::FusedIterator for LseExportRows {}

impl LseExportRows {
    /// Decode the next row, advancing through batches and row groups as they run out. `Ok(None)`
    /// means the file is exhausted.
    fn read_next(&mut self) -> Result<Option<(DateTime<Utc>, DataKind)>, LseError> {
        loop {
            let Some(cursor) = self.cursor.as_mut() else {
                if self.group >= self.reader.num_row_groups() {
                    return Ok(None);
                }
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

            return match (&mut cursor.columns, &self.plan) {
                (Columns::Candles(columns), Plan::Candles(plan, _)) => {
                    let series = self.candles.as_mut().ok_or_else(|| {
                        ParquetError::General("candle file without its checks".to_owned())
                    })?;
                    decode_candle(columns, plan, row, series, &mut self.decimal).map(Some)
                }
                (Columns::Trades(columns), Plan::Trades(plan)) => decode_trade(
                    columns,
                    plan,
                    row,
                    &mut self.previous_trade,
                    &mut self.decimal,
                )
                .map(Some),
                _ => Err(
                    ParquetError::General("export cursor does not match its plan".to_owned())
                        .into(),
                ),
            };
        }
    }
}

/// Decode and check one candle row.
fn decode_candle(
    columns: &mut CandleCursor,
    plan: &CandleColumns,
    row: usize,
    series: &mut CandleSeries,
    decimal: &mut DecimalText,
) -> Result<(DateTime<Utc>, DataKind), LseError> {
    // Every column is read before any check, so the cursors stay in step even on an error.
    let time = columns.time.next_value(row)?.copied();
    let open = columns.open.next_value(row)?.copied();
    let high = columns.high.next_value(row)?.copied();
    let low = columns.low.next_value(row)?.copied();
    let close = columns.close.next_value(row)?.copied();
    let volume = match &mut columns.volume {
        Some(column) => column.next_value(row)?.copied(),
        None => None,
    };
    let trades = match &mut columns.trades {
        Some(column) => column.next_value(row)?.copied(),
        None => None,
    };
    let date = match &mut columns.date {
        Some(column) => column.next_value(row)?.copied(),
        None => None,
    };

    let open_time = plan.unit.to_time(present(time, "time")?)?;
    let trades = trades
        .map(|count| {
            let unsigned = plan.trades.is_some_and(|(_, unsigned)| unsigned);
            if unsigned {
                // An unsigned `INT64` is stored in the signed physical type, bit for bit.
                Ok(u64::from_ne_bytes(count.to_ne_bytes()))
            } else {
                u64::try_from(count).map_err(|_| LseError::Deserialize {
                    message: format!("column trades: negative count {count}"),
                })
            }
        })
        .transpose()?;
    let session = date
        .map(|days| {
            DateTime::UNIX_EPOCH
                .date_naive()
                .checked_add_signed(TimeDelta::days(i64::from(days)))
                .ok_or_else(|| LseError::Deserialize {
                    message: format!("column date: {days} days from the epoch is not a date"),
                })
        })
        .transpose()?;

    let row = CandleRow {
        open_time,
        session,
        open: decimal.convert(present(open, "open")?)?,
        high: decimal.convert(present(high, "high")?)?,
        low: decimal.convert(present(low, "low")?)?,
        close: decimal.convert(present(close, "close")?)?,
        volume: volume.map(|volume| decimal.convert(volume)).transpose()?,
        trades,
    };
    let candle = series.accept(&row, None)?;

    Ok((candle.close_time, DataKind::Candle(candle)))
}

/// Decode and check one trade-slice row.
fn decode_trade(
    columns: &mut TradeCursor,
    plan: &TradeColumns,
    row: usize,
    previous: &mut Option<DateTime<Utc>>,
    decimal: &mut DecimalText,
) -> Result<(DateTime<Utc>, DataKind), LseError> {
    let time = columns.time.next_value(row)?.copied();
    let price = columns.price.next_value(row)?.copied();
    let size = columns.size.next_value(row)?.copied();
    let kind = present(columns.kind.next_value(row)?, "kind")?.as_utf8()?;
    if kind != TRADE_KIND {
        return Err(LseError::UnexpectedTradeKind {
            kind: kind.to_owned(),
        });
    }

    let time = plan.unit.to_time(present(time, "time")?)?;
    if let Some(previous) = *previous
        && time < previous
    {
        return Err(LseError::NonMonotonicTimestamps {
            previous,
            found: time,
        });
    }
    *previous = Some(time);

    let trade = PublicTrade {
        id: SmolStr::default(),
        price: decimal.convert(present(price, "price")?)?,
        amount: decimal.convert(present(size, "size")?)?,
        side: None,
    };

    Ok((time, DataKind::Trade(trade)))
}

/// Iterator over the market events decoded from one export file, from [`read_export`].
///
/// Ends at the first error, as [`LseExportRows`] does.
#[derive(Debug)]
pub struct LseExportEvents {
    rows: LseExportRows,
    exchange: ExchangeId,
    instrument: InstrumentIndex,
}

impl Iterator for LseExportEvents {
    type Item = Result<MarketEvent<InstrumentIndex, DataKind>, LseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(|row| {
            row.map(|(time, kind)| MarketEvent {
                time_exchange: time,
                time_received: time,
                exchange: self.exchange,
                instrument: self.instrument,
                kind,
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl std::iter::FusedIterator for LseExportEvents {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)] // Test code: panics on bad input are acceptable
mod tests {
    use super::*;
    use crate::subscription::candle::Candle;
    use parquet::data_type::ByteArray;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    use std::sync::Arc;

    /// The candle schema the provider writes, as measured (2026-11-28).
    const CANDLE_SCHEMA: &str = "message schema {
        REQUIRED INT64 time (TIMESTAMP(MILLIS, true));
        REQUIRED DOUBLE open; REQUIRED DOUBLE high; REQUIRED DOUBLE low; REQUIRED DOUBLE close;
        REQUIRED DOUBLE volume;
        REQUIRED INT64 trades (INTEGER(64, false));
    }";

    const DAILY_SCHEMA: &str = "message schema {
        REQUIRED INT64 time (TIMESTAMP(MILLIS, true));
        REQUIRED DOUBLE open; REQUIRED DOUBLE high; REQUIRED DOUBLE low; REQUIRED DOUBLE close;
        REQUIRED DOUBLE volume;
        REQUIRED INT64 trades (INTEGER(64, false));
        REQUIRED INT32 date (DATE);
    }";

    /// Option candles declare their prices `OPTIONAL`.
    const OPTION_CANDLE_SCHEMA: &str = "message schema {
        REQUIRED INT64 time (TIMESTAMP(MILLIS, true));
        OPTIONAL DOUBLE open; OPTIONAL DOUBLE high; OPTIONAL DOUBLE low; OPTIONAL DOUBLE close;
        REQUIRED DOUBLE volume;
        REQUIRED INT64 trades (INTEGER(64, false));
    }";

    const TRADE_SCHEMA: &str = "message schema {
        REQUIRED INT64 time (TIMESTAMP(NANOS, true));
        REQUIRED DOUBLE price; REQUIRED DOUBLE size;
        REQUIRED BYTE_ARRAY kind (STRING);
    }";

    /// One column's values for one row group, optional values as `None` for a null.
    enum Values {
        I64(Vec<i64>),
        I32(Vec<i32>),
        F64(Vec<Option<f64>>),
        Text(Vec<&'static str>),
    }

    /// Write a file with `schema`, one row group per entry of `groups`, each a column list in
    /// schema order.
    fn write(schema: &str, groups: Vec<Vec<Values>>) -> tempfile::NamedTempFile {
        write_schema(Arc::new(parse_message_type(schema).unwrap()), groups)
    }

    /// [`write`] for a schema the message syntax cannot spell.
    fn write_schema(schema: TypePtr, groups: Vec<Vec<Values>>) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut writer = SerializedFileWriter::new(
            file.reopen().unwrap(),
            schema,
            Arc::new(WriterProperties::builder().build()),
        )
        .unwrap();

        for columns in groups {
            let mut group = writer.next_row_group().unwrap();
            for values in columns {
                let mut column = group.next_column().unwrap().unwrap();
                match values {
                    Values::I64(values) => column
                        .typed::<Int64Type>()
                        .write_batch(&values, None, None)
                        .unwrap(),
                    Values::I32(values) => column
                        .typed::<Int32Type>()
                        .write_batch(&values, None, None)
                        .unwrap(),
                    Values::F64(values) => {
                        let present: Vec<f64> = values.iter().flatten().copied().collect();
                        let levels: Vec<i16> = values
                            .iter()
                            .map(|value| i16::from(value.is_some()))
                            .collect();
                        // Levels for an OPTIONAL column; a REQUIRED one has none to take.
                        let optional = column
                            .typed::<DoubleType>()
                            .get_descriptor()
                            .max_def_level()
                            > 0;
                        let levels = optional.then_some(levels.as_slice());
                        column
                            .typed::<DoubleType>()
                            .write_batch(&present, levels, None)
                            .unwrap()
                    }
                    Values::Text(values) => {
                        let values: Vec<ByteArray> =
                            values.into_iter().map(ByteArray::from).collect();
                        column
                            .typed::<ByteArrayType>()
                            .write_batch(&values, None, None)
                            .unwrap()
                    }
                };
                column.close().unwrap();
            }
            group.close().unwrap();
        }
        writer.close().unwrap();
        file
    }

    fn at(text: &str) -> DateTime<Utc> {
        chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .and_utc()
    }

    fn ms(text: &str) -> i64 {
        at(text).timestamp_millis()
    }

    fn some(values: &[f64]) -> Values {
        Values::F64(values.iter().copied().map(Some).collect())
    }

    /// A minute-candle row group: opens, then one price for every OHLC column.
    fn minute_group(opens: &[&str], price: f64) -> Vec<Values> {
        let n = opens.len();
        vec![
            Values::I64(opens.iter().map(|open| ms(open)).collect()),
            some(&vec![price; n]),
            some(&vec![price + 1.0; n]),
            some(&vec![price - 1.0; n]),
            some(&vec![price; n]),
            some(&vec![12.5; n]),
            Values::I64(vec![3; n]),
        ]
    }

    fn rows(
        file: &tempfile::NamedTempFile,
        data: LseExportData,
    ) -> Vec<Result<(DateTime<Utc>, DataKind), LseError>> {
        read_export_rows(file.path(), data).unwrap().collect()
    }

    fn candle(row: &Result<(DateTime<Utc>, DataKind), LseError>) -> (DateTime<Utc>, Candle) {
        match row {
            Ok((time, DataKind::Candle(candle))) => (*time, candle.clone()),
            other => panic!("candle expected, got {other:?}"),
        }
    }

    const MIN1: LseExportData = LseExportData::Candles(CandleInterval::Min1);

    #[test]
    fn minute_candles_are_stamped_at_their_close_with_exact_decimals() {
        let file = write(
            CANDLE_SCHEMA,
            vec![minute_group(
                &["2026-11-25 15:00:00", "2026-11-25 15:01:00"],
                277.81,
            )],
        );

        let rows = rows(&file, MIN1);

        assert_eq!(rows.len(), 2);
        let (time, first) = candle(&rows[0]);
        assert_eq!(time, at("2026-11-25 15:01:00"));
        assert_eq!(first.close_time, time);
        assert_eq!(first.open, Decimal::from_str("277.81").unwrap());
        assert_eq!(first.high, Decimal::from_str("278.81").unwrap());
        assert_eq!(first.volume, Some(Decimal::from_str("12.5").unwrap()));
        assert_eq!(first.trade_count, Some(3));
        assert_eq!(candle(&rows[1]).0, at("2026-11-25 15:02:00"));
    }

    #[test]
    fn a_price_keeps_every_digit_its_shortest_text_has() {
        // Sixteen significant digits: `Decimal::try_from(f64)` would round this to fifteen.
        let mut text = DecimalText::default();

        assert_eq!(
            text.convert(0.1234567890123456).unwrap(),
            Decimal::from_str("0.1234567890123456").unwrap()
        );
        assert_eq!(
            text.convert(1e-7).unwrap(),
            Decimal::from_str("0.0000001").unwrap()
        );
        assert!(matches!(
            text.convert(f64::NAN),
            Err(LseError::PriceNotRepresentable { .. })
        ));
    }

    #[test]
    fn daily_candles_carry_their_session_and_need_it() {
        let open = ms("2026-11-17 22:04:00");
        let file = write(
            DAILY_SCHEMA,
            vec![vec![
                Values::I64(vec![open]),
                some(&[1.1]),
                some(&[1.2]),
                some(&[1.0]),
                some(&[1.15]),
                some(&[4.0]),
                Values::I64(vec![9]),
                // 2026-11-18 is 20,775 days after the epoch.
                Values::I32(vec![20_775]),
            ]],
        );

        let rows = rows(&file, LseExportData::Candles(CandleInterval::Day1));

        let (time, _) = candle(&rows[0]);
        assert_eq!(time, at("2026-11-18 22:04:00"));

        // A file without `date` is not a daily file.
        let intraday = write(
            CANDLE_SCHEMA,
            vec![minute_group(&["2026-11-25 15:00:00"], 1.0)],
        );
        assert!(matches!(
            read_export_rows(
                intraday.path(),
                LseExportData::Candles(CandleInterval::Day1)
            ),
            Err(LseError::UnsupportedSchema { .. })
        ));
    }

    #[test]
    fn candles_decode_across_row_groups_and_stay_checked_across_them() {
        let file = write(
            CANDLE_SCHEMA,
            vec![
                minute_group(&["2026-11-25 15:00:00"], 1.0),
                minute_group(&["2026-11-25 15:01:00"], 2.0),
            ],
        );
        assert_eq!(
            rows(&file, MIN1).iter().filter(|row| row.is_ok()).count(),
            2
        );

        // The second group repeats the first group's bar: caught though each group is fine alone.
        let repeated = write(
            CANDLE_SCHEMA,
            vec![
                minute_group(&["2026-11-25 15:00:00"], 1.0),
                minute_group(&["2026-11-25 15:00:00"], 2.0),
            ],
        );
        let rows = rows(&repeated, MIN1);
        assert!(rows[0].is_ok());
        assert!(matches!(rows[1], Err(LseError::NonMonotonicCandles { .. })));
    }

    #[test]
    fn a_file_finer_than_declared_is_refused() {
        let file = write(
            CANDLE_SCHEMA,
            vec![minute_group(
                &["2026-11-25 15:00:00", "2026-11-25 15:01:00"],
                1.0,
            )],
        );

        let rows = rows(&file, LseExportData::Candles(CandleInterval::Hour1));

        assert!(rows[0].is_ok());
        assert!(matches!(
            rows[1],
            Err(LseError::UnexpectedCandleResolution { .. })
        ));
    }

    #[test]
    fn a_null_option_price_is_a_typed_error_and_ends_the_iterator() {
        let file = write(
            OPTION_CANDLE_SCHEMA,
            vec![vec![
                Values::I64(vec![ms("2026-11-25 15:00:00"), ms("2026-11-25 15:01:00")]),
                Values::F64(vec![Some(1.0), None]),
                some(&[1.0, 1.0]),
                some(&[1.0, 1.0]),
                some(&[1.0, 1.0]),
                some(&[1.0, 1.0]),
                Values::I64(vec![1, 1]),
            ]],
        );

        let mut iterator = read_export_rows(file.path(), MIN1).unwrap();

        assert!(iterator.next().unwrap().is_ok());
        assert!(matches!(
            iterator.next(),
            Some(Err(LseError::NullValue { column: "open" }))
        ));
        assert!(iterator.next().is_none());
    }

    #[test]
    fn trades_keep_nanoseconds_and_ties_and_are_sized_in_units() {
        let base = at("2026-11-25 15:00:00").timestamp_nanos_opt().unwrap();
        let file = write(
            TRADE_SCHEMA,
            vec![vec![
                Values::I64(vec![base + 1, base + 1, base + 7]),
                some(&[277.81, 277.82, 277.8]),
                some(&[100.0, 0.5, 3.0]),
                Values::Text(vec!["trade", "trade", "trade"]),
            ]],
        );

        let rows = rows(&file, LseExportData::TradeSlice);

        let decoded: Vec<_> = rows
            .into_iter()
            .map(|row| match row.unwrap() {
                (time, DataKind::Trade(trade)) => (time, trade),
                other => panic!("trade expected, got {other:?}"),
            })
            .collect();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].0, DateTime::from_timestamp_nanos(base + 1));
        assert_eq!(decoded[0].0, decoded[1].0);
        assert_eq!(decoded[1].1.amount, Decimal::from_str("0.5").unwrap());
        assert_eq!(decoded[2].1.price, Decimal::from_str("277.8").unwrap());
        assert_eq!(decoded[2].1.side, None);
        assert!(decoded[2].1.id.is_empty());
    }

    #[test]
    fn a_trade_stepping_back_or_of_another_kind_is_refused() {
        let base = at("2026-11-25 15:00:00").timestamp_nanos_opt().unwrap();
        let backwards = write(
            TRADE_SCHEMA,
            vec![vec![
                Values::I64(vec![base + 5, base + 4]),
                some(&[1.0, 1.0]),
                some(&[1.0, 1.0]),
                Values::Text(vec!["trade", "trade"]),
            ]],
        );
        assert!(matches!(
            rows(&backwards, LseExportData::TradeSlice)[1],
            Err(LseError::NonMonotonicTimestamps { .. })
        ));

        let quote = write(
            TRADE_SCHEMA,
            vec![vec![
                Values::I64(vec![base]),
                some(&[1.0]),
                some(&[1.0]),
                Values::Text(vec!["quote"]),
            ]],
        );
        assert!(matches!(
            &rows(&quote, LseExportData::TradeSlice)[0],
            Err(LseError::UnexpectedTradeKind { kind }) if kind == "quote"
        ));
    }

    #[test]
    fn the_timestamp_unit_is_read_from_the_schema() {
        // The same instant written in milliseconds (candles) and nanoseconds (trades).
        let instant = at("2026-11-25 15:00:00");
        let candles = write(
            CANDLE_SCHEMA,
            vec![minute_group(&["2026-11-25 15:00:00"], 1.0)],
        );
        let trades = write(
            TRADE_SCHEMA,
            vec![vec![
                Values::I64(vec![instant.timestamp_nanos_opt().unwrap()]),
                some(&[1.0]),
                some(&[1.0]),
                Values::Text(vec!["trade"]),
            ]],
        );

        assert_eq!(
            candle(&rows(&candles, MIN1)[0]).0,
            instant + TimeDelta::minutes(1)
        );
        assert_eq!(
            rows(&trades, LseExportData::TradeSlice)[0]
                .as_ref()
                .unwrap()
                .0,
            instant
        );
    }

    #[test]
    fn a_local_time_column_is_refused() {
        let file = write(
            "message schema {
                REQUIRED INT64 time (TIMESTAMP(MILLIS, false));
                REQUIRED DOUBLE open; REQUIRED DOUBLE high; REQUIRED DOUBLE low;
                REQUIRED DOUBLE close;
            }",
            vec![vec![
                Values::I64(vec![0]),
                some(&[1.0]),
                some(&[1.0]),
                some(&[1.0]),
                some(&[1.0]),
            ]],
        );

        assert!(matches!(
            read_export_rows(file.path(), MIN1),
            Err(LseError::UnsupportedColumnType { column, .. }) if column == "time"
        ));
    }

    #[test]
    fn a_trade_slice_decoded_as_candles_is_refused_and_greeks_are_not_decoded() {
        let trades = write(
            TRADE_SCHEMA,
            vec![vec![
                Values::I64(vec![0]),
                some(&[1.0]),
                some(&[1.0]),
                Values::Text(vec!["trade"]),
            ]],
        );

        assert!(matches!(
            read_export_rows(trades.path(), MIN1),
            Err(LseError::UnsupportedSchema { .. })
        ));
        assert!(matches!(
            read_export_rows(trades.path(), LseExportData::Greeks),
            Err(LseError::UnsupportedExportData { .. })
        ));
    }

    #[test]
    fn a_missing_file_is_an_io_error() {
        assert!(matches!(
            read_export_rows("/nonexistent/export.parquet", MIN1),
            Err(LseError::Io { .. })
        ));
    }

    #[test]
    fn events_carry_the_given_keys_and_the_row_time() {
        let file = write(
            CANDLE_SCHEMA,
            vec![minute_group(&["2026-11-25 15:00:00"], 1.0)],
        );

        let events: Vec<_> = read_export(
            file.path(),
            MIN1,
            ExchangeId::LseEquities,
            InstrumentIndex(7),
        )
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].exchange, ExchangeId::LseEquities);
        assert_eq!(events[0].instrument, InstrumentIndex(7));
        assert_eq!(events[0].time_exchange, at("2026-11-25 15:01:00"));
        assert_eq!(events[0].time_received, events[0].time_exchange);
    }

    /// The daily candle schema as a writer predating the logical-type union records it: converted
    /// types only. The message syntax reads `DATE` as the logical type, so `date` is built by hand.
    fn legacy_daily_schema() -> TypePtr {
        let parsed = parse_message_type(
            "message schema {
                REQUIRED INT64 time (TIMESTAMP_MILLIS);
                REQUIRED DOUBLE open; REQUIRED DOUBLE high; REQUIRED DOUBLE low;
                REQUIRED DOUBLE close; REQUIRED DOUBLE volume;
                REQUIRED INT64 trades (UINT_64);
            }",
        )
        .unwrap();
        let date = SchemaType::primitive_type_builder("date", PhysicalType::INT32)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::DATE)
            .build()
            .unwrap();
        let mut fields = parsed.get_fields().to_vec();
        fields.push(Arc::new(date));
        Arc::new(
            SchemaType::group_type_builder("schema")
                .with_fields(fields)
                .build()
                .unwrap(),
        )
    }

    /// One daily row whose `trades` cell holds `count`.
    fn daily_row(count: i64) -> Vec<Values> {
        vec![
            Values::I64(vec![ms("2026-11-17 22:04:00")]),
            some(&[1.1]),
            some(&[1.2]),
            some(&[1.0]),
            some(&[1.15]),
            some(&[4.0]),
            Values::I64(vec![count]),
            Values::I32(vec![20_775]),
        ]
    }

    #[test]
    fn a_schema_with_only_converted_types_is_read() {
        let schema = legacy_daily_schema();
        assert!(
            schema
                .get_fields()
                .iter()
                .all(|field| { field.get_basic_info().logical_type_ref().is_none() })
        );
        // An unsigned count past `i64::MAX` is stored as a negative `i64`, bit for bit.
        let file = write_schema(schema, vec![daily_row(-1)]);

        let daily = rows(&file, LseExportData::Candles(CandleInterval::Day1));

        let (time, bar) = candle(&daily[0]);
        assert_eq!(time, at("2026-11-18 22:04:00"));
        assert_eq!(bar.trade_count, Some(u64::MAX));

        let base = at("2026-11-25 15:00:00").timestamp_micros();
        let trades = write(
            "message schema {
                REQUIRED INT64 time (TIMESTAMP_MICROS);
                REQUIRED DOUBLE price; REQUIRED DOUBLE size;
                REQUIRED BYTE_ARRAY kind (UTF8);
            }",
            vec![vec![
                Values::I64(vec![base + 3]),
                some(&[1.0]),
                some(&[1.0]),
                Values::Text(vec!["trade"]),
            ]],
        );
        let decoded = rows(&trades, LseExportData::TradeSlice);
        assert_eq!(
            decoded[0].as_ref().unwrap().0,
            DateTime::from_timestamp_micros(base + 3).unwrap()
        );
    }

    #[test]
    fn a_count_is_read_as_its_declared_sign() {
        let unsigned = write(DAILY_SCHEMA, vec![daily_row(-1)]);
        let signed = write(
            &DAILY_SCHEMA.replace("INTEGER(64, false)", "INTEGER(64, true)"),
            vec![daily_row(-1)],
        );
        let daily = LseExportData::Candles(CandleInterval::Day1);

        assert_eq!(
            candle(&rows(&unsigned, daily)[0]).1.trade_count,
            Some(u64::MAX)
        );
        assert!(matches!(
            &rows(&signed, daily)[0],
            Err(LseError::Deserialize { message }) if message.contains("negative count")
        ));
    }

    #[test]
    fn unannotated_text_and_date_columns_are_refused() {
        let bare_kind = write(
            &TRADE_SCHEMA.replace("kind (STRING)", "kind"),
            vec![vec![
                Values::I64(vec![0]),
                some(&[1.0]),
                some(&[1.0]),
                Values::Text(vec!["trade"]),
            ]],
        );
        let bare_date = write(
            &DAILY_SCHEMA.replace("date (DATE)", "date"),
            vec![daily_row(1)],
        );

        assert!(matches!(
            read_export_rows(bare_kind.path(), LseExportData::TradeSlice),
            Err(LseError::UnsupportedColumnType { column, .. }) if column == "kind"
        ));
        assert!(matches!(
            read_export_rows(bare_date.path(), LseExportData::Candles(CandleInterval::Day1)),
            Err(LseError::UnsupportedColumnType { column, .. }) if column == "date"
        ));
    }

    #[test]
    fn a_timestamp_past_the_calendar_names_its_unit() {
        let file = write(
            CANDLE_SCHEMA,
            vec![vec![
                Values::I64(vec![i64::MAX]),
                some(&[1.0]),
                some(&[1.0]),
                some(&[1.0]),
                some(&[1.0]),
                some(&[1.0]),
                Values::I64(vec![1]),
            ]],
        );

        assert!(matches!(
            &rows(&file, MIN1)[0],
            Err(LseError::TimestampNotRepresentable {
                value: i64::MAX,
                unit: "milliseconds"
            })
        ));
    }

    #[test]
    fn the_iterators_report_what_is_left_and_where_they_are() {
        let file = write(
            CANDLE_SCHEMA,
            vec![minute_group(
                &["2026-11-25 15:00:00", "2026-11-25 15:01:00"],
                1.0,
            )],
        );

        let rows = read_export_rows(file.path(), MIN1).unwrap();
        assert!(format!("{rows:?}").starts_with("LseExportRows"));

        let mut events = read_export(
            file.path(),
            MIN1,
            ExchangeId::LseEquities,
            InstrumentIndex(0),
        )
        .unwrap();
        assert_eq!(events.size_hint(), (0, Some(2)));
        events.next().unwrap().unwrap();
        assert_eq!(events.size_hint(), (0, Some(1)));
    }

    #[test]
    fn the_events_iterator_can_cross_threads() {
        fn assert_send<T: Send + 'static>() {}
        assert_send::<LseExportEvents>();
        assert_send::<LseExportRows>();
    }
}
