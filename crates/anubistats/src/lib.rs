use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Record {
    pub id: u64,
    pub by: String,
    pub score: Option<u64>,
    pub time: Option<u64>,
    pub time_ts: String,
    pub title: String,
    pub url: String,
    pub text: String,
    pub deleted: Option<bool>,
    pub dead: Option<bool>,
    pub descendants: Option<i64>,
    pub author: String,
}

pub fn read_datasets() -> anyhow::Result<impl Iterator<Item = anyhow::Result<Record>>> {
    Ok(csv::Reader::from_path("stories-20230415.csv")?
        .into_deserialize()
        .map(|result| result.map_err(anyhow::Error::from)))
}
