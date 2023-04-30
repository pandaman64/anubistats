use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufRead, Read, Seek, SeekFrom},
    ops::Bound,
};

use anubistats_query::Query;
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
    }

    Ok(())
}
