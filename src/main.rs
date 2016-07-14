extern crate clap;
extern crate hyper;
extern crate serde_json;
extern crate chrono;

use std::fmt::{Display, Formatter};
use chrono::{DateTime, Local};

struct Request {
    stop_point_name: Option<String>
}

impl Request {
    fn with_stop_point_name(stop_point_name: String) -> Self {
        Request {
            stop_point_name: Some(stop_point_name)
        }
    }

    fn send(self, base_url: String) -> Result<Predictions, Error> {
        use std::io::{BufReader, BufRead};
        use hyper::client::Client;
        use hyper::status::StatusCode;
        use serde_json::Value;

        let mut args = vec!("ReturnList=StopPointName,LineName,DestinationText,EstimatedTime,TripID"
                            .to_string());
        if let Some(ref stop_point_name) = self.stop_point_name {
            args.push("StopPointName=".to_string() + stop_point_name.as_str());
        }
        let url = base_url + args.join("&").as_str();
        //println!("request url: {}", url);

        let client = Client::new();
        match client.get(&url).send() {
            Ok(response) => {
                match response.status {
                    StatusCode::Ok => {
                        let mut predictions: Vec<Prediction> = Vec::new();
                        let mut lines = BufReader::new(response).lines();
                        let ura_version_json =
                            serde_json::from_str::<Value>(lines.next().unwrap().unwrap().as_str()).unwrap();
                        let ura_version = ura_version_json.as_array().unwrap();
                        let timestamp = ura_version[2].as_i64().unwrap();
                        let time = datetime_from_millis(timestamp);
                        for line in lines {
                            let prediction_json: Value =
                                serde_json::from_str::<Value>(line.unwrap().as_str()).unwrap();
                            let prediction_array_json = prediction_json.as_array().unwrap();
                            let stop_point_name = prediction_array_json[1].as_string().unwrap().to_string();
                            let line_name = prediction_array_json[2].as_string().unwrap().to_string();
                            let destination_text = prediction_array_json[3].as_string().unwrap().to_string();
                            let trip_id = prediction_array_json[4].as_u64().unwrap();
                            let estimated_time =
                                datetime_from_millis(prediction_array_json[5].as_i64().unwrap());
                            predictions.push(Prediction {
                                stop_point_name: stop_point_name,
                                line_name: line_name,
                                destination_text: destination_text,
                                trip_id: trip_id,
                                estimated_time: estimated_time
                            });
                        }
                        Ok(Predictions{
                            time: time,
                            predictions: predictions
                        })
                    },
                    StatusCode::RangeNotSatisfiable => {
                        match self.stop_point_name {
                            Some(name) => {
                                Err(Error::BadStopPointName(name))
                            },
                            None => {
                                Err(Error::UnknownStatus(StatusCode::RangeNotSatisfiable))
                            }
                        }
                    }
                    unknown => {
                        Err(Error::UnknownStatus(unknown))
                    }
                }
            },
            Err(error) => {
                Err(Error::HyperError(error))
            }
        }
    }
}

fn datetime_from_millis(timestamp: i64) -> DateTime<Local> {
    use chrono::{NaiveDateTime, TimeZone};

    let secs: i64 = timestamp / 1000;
    let nsecs: u32 = (timestamp % 1000 * 1000_000) as u32;
    Local.from_utc_datetime(&NaiveDateTime::from_timestamp(secs, nsecs))
}

#[derive(Debug)]
enum Error {
    HyperError(hyper::error::Error),
    BadStopPointName(String),
    UnknownStatus(hyper::status::StatusCode)
}

#[derive(Debug)]
struct Predictions {
    time: DateTime<Local>,
    predictions: Vec<Prediction>
}

impl Display for Predictions {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        let now = self.time;
        for p in self.predictions.iter() {
            try!(writeln!(f, "{:>3}min {:>4} {}",
                          (p.estimated_time - now).num_minutes(), p.line_name, p.destination_text));
        }
        Ok(())
    }
}

trait PredictionsCombinator {
    fn intersect(self, ordered: bool) -> Option<Predictions>;
}

impl <S: IntoIterator<Item=Predictions>>PredictionsCombinator for S {
    fn intersect(self, ordered: bool) -> Option<Predictions> {
        use std::iter::FromIterator;
        use std::collections::HashMap;

        let mut iter = self.into_iter();
        match iter.next() {
            None => {
                None
            },
            Some(first_predictions) => {
                let time = first_predictions.time;
                let mut predictions_map: HashMap<TripId, Prediction> =
                    HashMap::from_iter(first_predictions.predictions.into_iter()
                                       .map(|p: Prediction| (p.trip_id, p)));

                for predictions in iter {
                    let mut new_predictions_map: HashMap<TripId, Prediction> = HashMap::new();
                    for p in predictions.predictions {
                        match predictions_map.remove(&p.trip_id) {
                            Some(pred) => {
                                if !ordered || (ordered && pred.estimated_time <= p.estimated_time) {
                                    new_predictions_map.insert(p.trip_id, pred);
                                }
                            },
                            None => {}
                        }
                    }
                    predictions_map = new_predictions_map;
                }

                let mut predictions_vec = predictions_map
                    .drain().map(|(_, p)| p).collect::<Vec<Prediction>>();
                predictions_vec.sort_by_key(|p| p.estimated_time);
                Some(Predictions{
                    time: time,
                    predictions: predictions_vec
                })
            }
        }
    }
}

type TripId = u64;

#[derive(Clone, Debug)]
struct Prediction {
    stop_point_name: String,
    line_name: String,
    destination_text: String,
    trip_id: TripId,
    estimated_time: DateTime<Local>
}

fn main() {
    use clap::{App, Arg};

    let arg_matches = App::new("travel_ura")
        .about("Queries URA live bus APIs, like the one of Transport for London (TfL)")
        .arg(Arg::with_name("STOP")
             .takes_value(true)
             .multiple(true))
        .get_matches();

    let base_url = "http://ivu.aseag.de/interfaces/ura/instant_V1?";

    let stops: Vec<&str> = arg_matches.values_of("STOP").unwrap().collect();
    //println!("listing busses visiting stops {}", stops.join(", "));
    let results = stops.iter().map(|stop| {
        match Request::with_stop_point_name(stop.to_string()).send(base_url.to_string()) {
            Ok(results) => {
                //println!("{}: {:#?}", stop, results);
                results
            },
            Err(error) => {
                println!("error: {:?}", error);
                std::process::exit(1);
            }
        }
    });
    let intersection = results.intersect(true).unwrap();
    println!("{}", intersection);
}

