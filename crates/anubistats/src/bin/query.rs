//! This binary provides a REPL for querying the index created by crates/anubistats/src/bin/index.rs.

use std::{
    collections::{hash_map::Entry, HashMap},
    fs::File,
    io::BufRead,
    sync::Arc,
};

use anubistats_query::Query;
use arrow::{
    array::{
        Array, ArrayBuilder, AsArray, BinaryArray, BooleanArray, StringArray, StringBuilder,
        UInt32Array, UInt64Array, UInt64Builder,
    },
    datatypes::DataType,
    row::{RowConverter, SortField},
};
use parquet::{
    arrow::{
        arrow_reader::{
            ArrowPredicateFn, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowFilter,
            RowSelection, RowSelector,
        },
        ProjectionMask,
    },
    file::page_index::index::Index,
};
use roaring::RoaringBitmap;

fn find_postings_list_parquet(word: &str) -> anyhow::Result<RoaringBitmap> {
    let word = word.to_string();
    let file = File::open("postings_lists.parquet")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
        file,
        ArrowReaderOptions::new().with_page_index(true),
    )?;

    let metadata = builder.metadata();
    let offset_indexes = metadata.offset_indexes().unwrap();
    let page_indexes = metadata.page_indexes().unwrap();
    let word_column_index = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|column| column.name() == "word")
        .unwrap();

    let mut selectors = vec![];

    // ASSUMPTION:
    // 1. The index is byte array index
    // 2. The index is sorted in ascending order
    for row_group in 0..offset_indexes.len() {
        let offset_index = &offset_indexes[row_group][word_column_index];
        let page_index = &page_indexes[row_group][word_column_index];
        let row_group_end = builder.metadata().row_group(row_group).num_rows();

        match page_index {
            Index::BYTE_ARRAY(index) => {
                match index.indexes.binary_search_by(|page_index| {
                    let min = page_index.min.as_ref().unwrap().data();
                    let max = page_index.max.as_ref().unwrap().data();
                    let needle = word.as_bytes();

                    if min > needle {
                        std::cmp::Ordering::Greater
                    } else if max < needle {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Equal
                    }
                }) {
                    Ok(idx) => {
                        let found_page_location = &offset_index[idx];
                        let select_start = found_page_location.first_row_index;
                        let select_end = if idx + 1 < offset_index.len() {
                            offset_index[idx + 1].first_row_index
                        } else {
                            row_group_end
                        };

                        selectors.extend([
                            RowSelector::skip(select_start.try_into().unwrap()),
                            RowSelector::select((select_end - select_start).try_into().unwrap()),
                            RowSelector::skip((row_group_end - select_end).try_into().unwrap()),
                        ]);
                    }
                    Err(_) => {
                        selectors.push(RowSelector::skip(row_group_end.try_into().unwrap()));
                    }
                };
            }
            _ => unreachable!(),
        }
    }

    let predicate = ArrowPredicateFn::new(
        ProjectionMask::leaves(
            builder.parquet_schema(),
            std::iter::once(
                builder
                    .parquet_schema()
                    .columns()
                    .iter()
                    .position(|c| c.name() == "word")
                    .unwrap(),
            ),
        ),
        move |batch| {
            let words: &StringArray = batch.column(0).as_string();
            arrow::compute::eq_utf8_scalar(words, word.as_str())
        },
    );
    let row_filter = RowFilter::new(vec![Box::new(predicate)]);
    let mut reader = builder
        .with_row_selection(RowSelection::from(selectors))
        .with_row_filter(row_filter)
        .build()?;

    if let Some(batch) = reader.next() {
        let batch = batch?;
        if batch.num_rows() > 0 {
            let postings_lists: &BinaryArray = batch["postings_list"].as_binary();
            let postings_list_bytes = postings_lists.value(0);
            let postings_list = RoaringBitmap::deserialize_from(postings_list_bytes)?;

            Ok(postings_list)
        } else {
            Ok(RoaringBitmap::new())
        }
    } else {
        Ok(RoaringBitmap::new())
    }
}

fn eval_query<F>(query: &Query, find_postings_list: &F) -> anyhow::Result<RoaringBitmap>
where
    F: Fn(&str) -> anyhow::Result<RoaringBitmap>,
{
    match query {
        anubistats_query::Query::Word(word) => Ok(find_postings_list(word)?),
        anubistats_query::Query::And(lhs, rhs) => {
            let lhs = eval_query(lhs, find_postings_list)?;
            let rhs = eval_query(rhs, find_postings_list)?;
            Ok(lhs & rhs)
        }
        anubistats_query::Query::Or(lhs, rhs) => {
            let lhs = eval_query(lhs, find_postings_list)?;
            let rhs = eval_query(rhs, find_postings_list)?;
            Ok(lhs | rhs)
        }
    }
}

struct Document {
    roaring_id: u32,
    doc_id: u64,
    title: String,
}

fn retrieve_stored_fields(roaring_ids_filter: RoaringBitmap) -> anyhow::Result<Vec<Document>> {
    let len = roaring_ids_filter.len();

    let file = File::open("stored_fields.parquet")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Construct a reader that only reads the rows that have matching roaring IDs.
    let predicate = ArrowPredicateFn::new(
        ProjectionMask::leaves(
            builder.parquet_schema(),
            std::iter::once(
                builder
                    .parquet_schema()
                    .columns()
                    .iter()
                    .position(|c| c.name() == "id")
                    .unwrap(),
            ),
        ),
        move |batch| {
            let roaring_ids: &UInt32Array = batch.column(0).as_primitive();
            Ok(BooleanArray::from_unary(roaring_ids, |roaring_id| {
                roaring_ids_filter.contains(roaring_id)
            }))
        },
    );
    let row_filter = RowFilter::new(vec![Box::new(predicate)]);
    let reader = builder.with_row_filter(row_filter).build()?;

    let mut documents = Vec::with_capacity(len.try_into()?);
    for batch in reader {
        let batch = batch?;
        let roaring_ids: &UInt32Array = batch["id"].as_primitive();
        let doc_ids: &UInt64Array = batch["doc_id"].as_primitive();
        let title: &StringArray = batch["title"].as_string();

        for i in 0..batch.num_rows() {
            let roaring_id = roaring_ids.value(i);
            let doc_id = doc_ids.value(i);
            let title = title.value(i);

            documents.push(Document {
                roaring_id,
                doc_id,
                title: title.to_string(),
            });
        }
    }
    Ok(documents)
}

struct ScoresGroupedByDate {
    date: StringArray,
    score: UInt64Array,
    count: UInt64Array,
}

fn group_scores_by_date(roaring_ids_filter: RoaringBitmap) -> anyhow::Result<ScoresGroupedByDate> {
    let file = File::open("stored_fields.parquet")?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Construct a reader that only reads the rows that have matching roaring IDs.
    let predicate = ArrowPredicateFn::new(
        ProjectionMask::leaves(
            builder.parquet_schema(),
            std::iter::once(
                builder
                    .parquet_schema()
                    .columns()
                    .iter()
                    .position(|c| c.name() == "id")
                    .unwrap(),
            ),
        ),
        move |batch| {
            let roaring_ids: &UInt32Array = batch.column(0).as_primitive();
            Ok(BooleanArray::from_unary(roaring_ids, |roaring_id| {
                roaring_ids_filter.contains(roaring_id)
            }))
        },
    );
    let row_filter = RowFilter::new(vec![Box::new(predicate)]);
    let reader = builder.with_row_filter(row_filter).build()?;

    let mut row_converter = RowConverter::new(vec![SortField::new(DataType::Utf8)])?;
    let mut row_to_index = HashMap::new();
    let mut date_builder = StringBuilder::new();
    let mut sum_scores_builder = UInt64Builder::new();
    let mut count_builder = UInt64Builder::new();

    for batch in reader {
        let batch = batch?;

        let dates = &batch["date"];
        let scores: &UInt64Array = batch["score"].as_primitive();

        let keys = row_converter.convert_columns(&[Arc::clone(dates)])?;
        for (i, key) in keys.iter().enumerate() {
            let score = if !scores.is_null(i) {
                scores.value(i)
            } else {
                0
            };

            match row_to_index.entry(key.owned()) {
                Entry::Occupied(entry) => {
                    let index = *entry.get();
                    sum_scores_builder.values_slice_mut()[index] += score;
                    count_builder.values_slice_mut()[index] += 1;
                }
                Entry::Vacant(entry) => {
                    let index = sum_scores_builder.len();
                    entry.insert(index);
                    sum_scores_builder.append_value(score);
                    count_builder.append_value(1);

                    let dates: &StringArray = dates.as_string();
                    date_builder.append_value(dates.value(i));
                }
            }
        }
    }

    Ok(ScoresGroupedByDate {
        date: date_builder.finish(),
        score: sum_scores_builder.finish(),
        count: count_builder.finish(),
    })
}

fn measure_time<F, R>(f: F) -> (f64, R)
where
    F: FnOnce() -> R,
{
    let start = std::time::Instant::now();
    let result = f();
    let end = std::time::Instant::now();
    let duration = end - start;
    let duration = duration.as_secs_f64();
    (duration, result)
}

fn main() -> anyhow::Result<()> {
    // REPL for querying the postings lists.
    println!("Enter a query:");
    let stdin = std::io::stdin().lock();
    for line in stdin.lines() {
        let line = line?;
        let query = line.trim();
        let query = match anubistats_query::parse(query) {
            Ok(query) => query,
            Err(_) => {
                eprintln!("parse error");
                continue;
            }
        };

        let (eval_query_time, postings_lists) =
            measure_time(|| eval_query(&query, &find_postings_list_parquet));
        let postings_lists = postings_lists?;

        eprintln!("Evaluated query in {:.8} ms", eval_query_time * 1000.0);

        println!(
            "{} documents match the query '{:?}'",
            postings_lists.len(),
            query
        );

        let documents = retrieve_stored_fields(postings_lists.clone())?;
        for document in documents.iter().take(5) {
            println!(
                "[{}] {}: {}",
                document.roaring_id, document.doc_id, document.title
            );
        }

        println!("How many scores the matched documents have on each date?");

        let group_by_result = group_scores_by_date(postings_lists)?;
        for i in 0..5 {
            println!(
                "{}: {} ({} documents)",
                group_by_result.date.value(i),
                group_by_result.score.value(i),
                group_by_result.count.value(i)
            );
        }
    }

    Ok(())
}
