//! This binary provides a REPL for querying the index created by crates/anubistats/src/bin/index.rs.

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, Read, Seek, SeekFrom},
    ops::Bound,
};

use anubistats_query::Query;
use arrow::array::{AsArray, BooleanArray, StringArray, UInt32Array, UInt64Array};
use parquet::arrow::{
    arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter},
    ProjectionMask,
};
use roaring::RoaringBitmap;

fn find_offset_and_length(
    offsets: &BTreeMap<String, usize>,
    query: &str,
) -> Option<(usize, usize)> {
    let mut range = offsets.range::<str, _>((Bound::Included(query), Bound::Unbounded));
    let (first_word, offset) = range.next()?;
    let (_, next_offset) = range.next()?;

    if first_word == query {
        let length = next_offset - offset;
        Some((*offset, length))
    } else {
        None
    }
}

fn find_postings_list(
    word: &str,
    mut postings_lists_file: &File,
    offsets: &BTreeMap<String, usize>,
) -> anyhow::Result<RoaringBitmap> {
    if let Some((offset, length)) = find_offset_and_length(offsets, word) {
        postings_lists_file.seek(SeekFrom::Start(offset.try_into()?))?;
        let postings_list =
            RoaringBitmap::deserialize_from(postings_lists_file.take(length.try_into()?))?;

        Ok(postings_list)
    } else {
        Ok(RoaringBitmap::new())
    }
}

fn eval_query(
    query: &Query,
    postings_lists_file: &File,
    offsets: &BTreeMap<String, usize>,
) -> anyhow::Result<RoaringBitmap> {
    match query {
        anubistats_query::Query::Word(word) => {
            Ok(find_postings_list(word, postings_lists_file, offsets)?)
        }
        anubistats_query::Query::And(lhs, rhs) => {
            let lhs = eval_query(lhs, postings_lists_file, offsets)?;
            let rhs = eval_query(rhs, postings_lists_file, offsets)?;
            Ok(lhs & rhs)
        }
        anubistats_query::Query::Or(lhs, rhs) => {
            let lhs = eval_query(lhs, postings_lists_file, offsets)?;
            let rhs = eval_query(rhs, postings_lists_file, offsets)?;
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

fn main() -> anyhow::Result<()> {
    // Read postings lists and index from disk.
    let postings_lists_file = File::open("postings_lists.bin")?;
    let offsets: BTreeMap<String, usize> =
        serde_json::from_reader(File::open("postings_lists_offsets.json")?)?;

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

        let postings_lists = eval_query(&query, &postings_lists_file, &offsets)?;
        println!(
            "{} documents match the query '{:?}'",
            postings_lists.len(),
            query
        );

        let documents = retrieve_stored_fields(postings_lists)?;
        for document in documents.iter().take(10) {
            println!(
                "[{}] {}: {}",
                document.roaring_id, document.doc_id, document.title
            );
        }
    }

    Ok(())
}
