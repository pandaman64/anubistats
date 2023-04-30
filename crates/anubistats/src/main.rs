//! In this example, we create postings lists for each word in the dataset.

mod repl;

use std::{collections::BTreeMap, fs::File, io::BufWriter};

use anubistats::read_datasets;
use roaring::RoaringBitmap;

fn main() -> anyhow::Result<()> {
    // Construct postings lists from the words in the titles.
    let mut postings_lists = BTreeMap::new();
    let mut roaring_id = 0;
    for record in read_datasets()? {
        let record = record?;
        for word in record.title.split_whitespace() {
            let word = word.to_lowercase();
            if !word.is_empty() {
                let postings_list = postings_lists
                    .entry(word)
                    .or_insert_with(RoaringBitmap::new);
                assert!(postings_list.push(roaring_id));
                roaring_id += 1;
            }
        }
    }

    let postings_lists_file = File::create("postings_lists.bin")?;
    let mut postings_lists_writer = BufWriter::new(postings_lists_file);
    let mut postings_lists_offsets = BTreeMap::new();

    let mut offset = 0;
    for (word, postings_list) in postings_lists {
        postings_list.serialize_into(&mut postings_lists_writer)?;
        postings_lists_offsets.insert(word, offset);
        offset += postings_list.serialized_size();
    }

    let postings_lists_offsets_file = File::create("postings_lists_offsets.json")?;
    serde_json::to_writer_pretty(postings_lists_offsets_file, &postings_lists_offsets)?;

    // REPL for querying the postings lists.
    // let mut input = String::new();
    // loop {
    //     println!("Enter a word to query:");
    //     input.clear();
    //     std::io::stdin().read_line(&mut input)?;
    //     let input = input.trim();
    //     if input.is_empty() {
    //         break;
    //     }
    //     let postings_list = postings_lists.get(input);
    //     if let Some(postings_list) = postings_list {
    //         println!("{} documents contain the word '{}'", postings_list.len(), input);
    //         // println!("The documents are:");
    //         // for id in postings_list {
    //         //     println!("  {}", id);
    //         // }
    //     } else {
    //         println!("No documents contain the word '{}'", input);
    //     }
    // }

    Ok(())
}
