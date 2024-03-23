use chrono::NaiveDate;
use clap::Parser;
use csv::{Reader, Writer};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::task::{JoinError, JoinSet};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    all_reservoirs: bool,
    #[arg(long)]
    reservoir: Option<String>,
    #[arg(long)]
    start_date: String,
    #[arg(long)]
    end_date: String,
    #[arg(long)]
    output_file: PathBuf,
    #[arg(long)]
    reservoir_data: PathBuf,
}

#[derive(Debug, Deserialize, Serialize)]
struct CdecData {
    station_id: String,
    duration: String,
    #[serde(deserialize_with = "parse_cdec_date")]
    date_time: NaiveDate,
    #[serde(deserialize_with = "parse_cdec_date")]
    obs_date: NaiveDate,
    value: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct Reservoir {
    id: String,
    dam: String,
    lake: String,
    stream: String,
    capacity: u32,
    year_fill: i32,
}

#[derive(Debug)]
enum FetchError {
    RequestError(reqwest::Error),
    MaxRetriesExceeded(String),
    ParseError(csv::Error),
}

impl From<reqwest::Error> for FetchError {
    fn from(err: reqwest::Error) -> Self {
        FetchError::RequestError(err)
    }
}

impl From<csv::Error> for FetchError {
    fn from(err: csv::Error) -> Self {
        FetchError::ParseError(err)
    }
}

#[derive(Debug)]
enum MainError {
    IoError(std::io::Error),
    CsvError(csv::Error),
    FetchError(FetchError),
    JoinError(JoinError),
}

impl From<std::io::Error> for MainError {
    fn from(err: std::io::Error) -> Self {
        MainError::IoError(err)
    }
}

impl From<csv::Error> for MainError {
    fn from(err: csv::Error) -> Self {
        MainError::CsvError(err)
    }
}

impl From<FetchError> for MainError {
    fn from(err: FetchError) -> Self {
        MainError::FetchError(err)
    }
}

impl From<JoinError> for MainError {
    fn from(err: JoinError) -> Self {
        MainError::JoinError(err)
    }
}

fn parse_cdec_date<'de, D>(deserializer: D) -> Result<NaiveDate, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    NaiveDate::parse_from_str(&s, "%Y%m%d %H%M").map_err(serde::de::Error::custom)
}

async fn fetch_data(
    client: &Client,
    station_id: &str,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<CdecData>, FetchError> {
    let mut data = Vec::new();

    for &duration in &["D", "M"] {
        let mut retry_count = 0;
        loop {
            let url = format!(
                "https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?Stations={}&SensorNums=15&dur_code={}&Start={}&End={}",
                station_id, duration, start_date, end_date
            );

            let response = client.get(&url).send().await?;
            let status = response.status();

            if status.is_success() {
                let datum = response.text().await?;
                let mut rdr = Reader::from_reader(datum.as_bytes());
                data = rdr
                    .deserialize()
                    .filter_map(|result| result.ok())
                    .collect::<Vec<CdecData>>();
                break;
            } else {
                retry_count += 1;
                if retry_count >= 10 {
                    return Err(FetchError::MaxRetriesExceeded(format!(
                        "Max retries exceeded for station ID: {}",
                        station_id
                    )));
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(2u64.pow(retry_count))).await;
            }
        }

        if !data.is_empty() {
            break;
        }
    }

    Ok(data)
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    let args = Args::parse();

    let reservoir_data = std::fs::File::open(args.reservoir_data)?;
    let reservoirs: HashMap<String, Reservoir> = csv::Reader::from_reader(reservoir_data)
        .deserialize()
        .filter_map(|result| result.ok())
        .map(|res: Reservoir| (res.id.clone(), res))
        .collect();

    let station_ids = if args.all_reservoirs {
        reservoirs.keys().cloned().collect::<Vec<_>>()
    } else {
        args.reservoir
            .unwrap()
            .split(',')
            .map(|s| s.trim().to_owned())
            .collect()
    };

    let mut join_set = JoinSet::new();
    for station_id in station_ids {
        let client = Client::new();
        let start_date = args.start_date.clone();
        let end_date = args.end_date.clone();
        join_set.spawn(
            async move {
                fetch_data(&client, &station_id, &start_date, &end_date)
                    .await
                    .map_err(MainError::from)
            }
        );
    }

    let mut data: Vec<CdecData> = Vec::new();
    while let Some(res) = join_set.join_next().await {
        // let res1 = res?;
        data.extend(res??);
    }

    data.iter_mut().for_each(|d| {
        if let Some(reservoir) = reservoirs.get(&d.station_id) {
            if d.value > (reservoir.capacity as f64 * 1.01) as u32 {
                d.value = reservoir.capacity;
            }
        }
    });

    data.sort_by(|a, b| {
        a.station_id
            .cmp(&b.station_id)
            .then(a.date_time.cmp(&b.date_time))
    });

    let mut wtr = Writer::from_path(args.output_file)?;
    for d in data {
        wtr.serialize(d)?;
    }
    wtr.flush()?;

    Ok(())
}
