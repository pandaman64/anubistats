//! This binary creates the following data files to facillitate the queries:
//!
//! 1. The inverted index for words in the Hacker News titles.
//!    The file maps words to the offset of the postings list for that word in the postings lists file.
//! 2. The postings list for each word in the Hacker News titles.
//! 3. The columnar store for the Hacker News entries to show the info of each entry.

use std::{collections::BTreeMap, fs::File, sync::Arc};

use anubistats::read_datasets;
use arrow::{
    array::{BinaryBuilder, StringBuilder, UInt32Builder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::arrow::ArrowWriter;
use roaring::RoaringBitmap;

fn main() -> anyhow::Result<()> {
    // Construct postings lists from the words in the titles.
    let mut postings_lists = BTreeMap::new();
    let mut id_builder = UInt32Builder::new();
    let mut doc_id_builder = UInt64Builder::new();
    let mut title_builder = StringBuilder::new();

    for (roaring_id, record) in (read_datasets()?).enumerate() {
        let record = record?;

        // Add to postings lists
        for word in record.title.split_whitespace() {
            let word = word.to_lowercase();
            if !word.is_empty() {
                let postings_list = postings_lists
                    .entry(word)
                    .or_insert_with(RoaringBitmap::new);
                postings_list.push(roaring_id.try_into()?);
            }
        }

        // Add to columnar store
        id_builder.append_value(roaring_id.try_into()?);
        doc_id_builder.append_value(record.id);
        title_builder.append_value(record.title);
    }

    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("doc_id", DataType::UInt64, false),
        Field::new("title", DataType::Utf8, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(doc_id_builder.finish()),
            Arc::new(title_builder.finish()),
        ],
    )?;

    let mut word_builder = StringBuilder::new();
    let mut postings_list_builder = BinaryBuilder::new();

    for (word, postings_list) in postings_lists {
        let mut buffer = Vec::with_capacity(postings_list.serialized_size());
        postings_list.serialize_into(&mut buffer)?;

        word_builder.append_value(word);
        postings_list_builder.append_value(buffer);
    }

    let stored_fields_file = File::create("stored_fields.parquet")?;
    let mut writer = ArrowWriter::try_new(stored_fields_file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    let word_offset_schema = Schema::new(vec![
        Field::new("word", DataType::Utf8, false),
        Field::new("postings_list", DataType::Binary, false),
    ]);
    let word_batch = RecordBatch::try_new(
        Arc::new(word_offset_schema),
        vec![
            Arc::new(word_builder.finish()),
            Arc::new(postings_list_builder.finish()),
        ],
    )?;

    let postings_lists_file = File::create("postings_lists.parquet")?;
    let mut writer = ArrowWriter::try_new(postings_lists_file, word_batch.schema(), None)?;
    writer.write(&word_batch)?;
    writer.close()?;

    Ok(())
}
