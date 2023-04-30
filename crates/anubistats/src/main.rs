//! In this example, we create postings lists for each word in the dataset.

use std::collections::HashMap;

use anubistats::read_datasets;

fn main() -> anyhow::Result<()> {
    // Construct postings lists from the words in the titles.
    let mut postings_lists = HashMap::new();
    for record in read_datasets()? {
        let record = record?;
        for word in record.title.split_whitespace() {
            let word = word.to_lowercase();
            if !word.is_empty() {
                let postings_list = postings_lists.entry(word).or_insert_with(Vec::new);
                postings_list.push(record.id);
            }
        }
    }

    // REPL for querying the postings lists.
    let mut input = String::new();
    loop {
        println!("Enter a word to query:");
        input.clear();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim();
        if input.is_empty() {
            break;
        }
        let postings_list = postings_lists.get(input);
        if let Some(postings_list) = postings_list {
            println!("{} documents contain the word '{}'", postings_list.len(), input);
            // println!("The documents are:");
            // for id in postings_list {
            //     println!("  {}", id);
            // }
        } else {
            println!("No documents contain the word '{}'", input);
        }
    }

    Ok(())
}
