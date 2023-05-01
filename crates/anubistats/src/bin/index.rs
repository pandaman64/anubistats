//! This binary creates the following data files to facillitate the queries:
//! 
//! 1. The inverted index for words in the Hacker News titles.
//!    The file maps words to the offset of the postings list for that word in the postings lists file.
//! 2. The postings list for each word in the Hacker News titles.
//! 3. The columnar store for the Hacker News entries to show the info of each entry.

use std::{collections::BTreeMap, fs::File, io::BufWriter, sync::Arc};

use anubistats::read_datasets;
use arrow::{
    array::{StringBuilder, UInt32Builder, UInt64Builder},
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

    let postings_lists_file = File::create("postings_lists.bin")?;
    let mut postings_lists_writer = BufWriter::new(postings_lists_file);
    let mut postings_lists_offsets = BTreeMap::new();

    let mut word_builder = StringBuilder::new();
    let mut offset_builder = UInt64Builder::new();
    let mut length_builder = UInt64Builder::new();

    let mut offset = 0;
    for (word, postings_list) in postings_lists {
        postings_list.serialize_into(&mut postings_lists_writer)?;
        postings_lists_offsets.insert(word.clone(), offset);
        offset += postings_list.serialized_size();

        word_builder.append_value(word);
        offset_builder.append_value(offset.try_into()?);
        length_builder.append_value(postings_list.serialized_size().try_into()?);
    }

    let postings_lists_offsets_file = File::create("postings_lists_offsets.json")?;
    serde_json::to_writer(postings_lists_offsets_file, &postings_lists_offsets)?;

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

    let ids_file = File::create("stored_fields.parquet")?;
    let mut writer = ArrowWriter::try_new(ids_file, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;

    let word_offset_schema = Schema::new(vec![
        Field::new("word", DataType::Utf8, false),
        Field::new("offset", DataType::UInt64, false),
        Field::new("length", DataType::UInt64, false),
    ]);
    let word_batch = RecordBatch::try_new(
        Arc::new(word_offset_schema),
        vec![
            Arc::new(word_builder.finish()),
            Arc::new(offset_builder.finish()),
            Arc::new(length_builder.finish()),
        ],
    )?;

    let postings_list_offsets_parquet_file = File::create("postings_lists_offsets.parquet")?;
    let mut writer = ArrowWriter::try_new(
        postings_list_offsets_parquet_file,
        word_batch.schema(),
        None,
    )?;
    writer.write(&word_batch)?;
    writer.close()?;

    Ok(())
}
