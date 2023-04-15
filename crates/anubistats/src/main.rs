use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    fs,
};

use anubistats::read_datasets;

fn count_words<I: Iterator<Item = anyhow::Result<String>>>(
    text: I,
    stopwords: &HashSet<&str>,
) -> anyhow::Result<HashMap<String, i64>> {
    let mut words = HashMap::new();

    for fragment in text {
        let fragment = fragment?;
        for word in fragment.split_whitespace() {
            if stopwords.contains(word) {
                continue;
            }
            let word = word
                .trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase();
            if !word.is_empty() && !stopwords.contains(word.as_str()) {
                *words.entry(word).or_insert(0i64) += 1;
            }
        }
    }

    Ok(words)
}

fn count_frequent_words(words: HashMap<String, i64>, limit: usize) -> Vec<(i64, String)> {
    let mut frequent_words = BinaryHeap::new();
    for (word, count) in words {
        frequent_words.push((-count, word));
        if frequent_words.len() > limit {
            frequent_words.pop();
        }
    }
    let mut frequent_words = frequent_words.into_vec();
    frequent_words.sort_unstable();
    frequent_words
}

fn main() -> anyhow::Result<()> {
    let stopwords = fs::read_to_string("stopwords-en.txt")?;
    let stopwords: HashSet<_> = stopwords.lines().map(|line| line.trim()).collect();

    let records = read_datasets()?;
    let title_words = count_words(
        records.map(|result| result.map(|record| record.title)),
        &stopwords,
    )?;
    println!("{} unique words in titles", title_words.len());
    let frequent_title_words = count_frequent_words(title_words, 10);
    println!("Most frequent words in titles:");
    for (count, word) in frequent_title_words {
        println!("{}: {}", word, -count);
    }

    let records = read_datasets()?;
    let text_words = count_words(
        records.map(|result| result.map(|record| record.text)),
        &stopwords,
    )?;
    println!("{} unique words in text", text_words.len());
    let frequent_text_words = count_frequent_words(text_words, 10);
    println!("Most frequent words in text:");
    for (count, word) in frequent_text_words {
        println!("{}: {}", word, -count);
    }

    Ok(())
}
