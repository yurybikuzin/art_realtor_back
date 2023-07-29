use super::*;

pub struct ElasticRequest {
    client: Client,
    host: String,
    scroll_timeout: u64,
    kind: ElasticRequestKind,
}

pub struct ElasticRequestArg {
    pub host: String,
    pub index_url_part: String,
    pub query: Value,
    pub fields: Vec<String>,
    pub fetch_limit: usize,
    pub scroll_timeout: u64,
}

pub enum ElasticRequestFetchRet {
    Break,
    Continue,
}

pub trait ElasticContentTrait<S: DeserializeOwned> {
    fn extend(&mut self, source: Vec<S>, scan_start: SystemTime);
    fn fields(&self) -> Vec<String>;
}

impl ElasticRequest {
    pub fn processed_count(&self) -> usize {
        self.kind.processed_count()
    }
    pub fn scan_start(&self) -> Option<SystemTime> {
        self.kind.scan_start()
    }
    pub fn new(arg: ElasticRequestArg) -> Self {
        let ElasticRequestArg {
            host,
            index_url_part,
            query,
            fields,
            fetch_limit,
            scroll_timeout,
        } = arg;
        Self {
            client: Client::builder().gzip(true).build().unwrap(),
            host,
            scroll_timeout,
            kind: ElasticRequestKind::First {
                index_url_part,
                query,
                fields, //: index.get_fields(),
                fetch_limit,
            },
        }
    }
    pub fn duration(&self, use_prev_fetch_end: bool) -> Duration {
        match self.kind {
            ElasticRequestKind::First { .. } => Duration::from_millis(0),
            ElasticRequestKind::Next {
                prev_fetch_end,
                prev_fetch_start,
                scan_start,
                ..
            } => if use_prev_fetch_end {
                prev_fetch_end
            } else {
                prev_fetch_start
            }
            .duration_since(scan_start)
            .unwrap(),
        }
    }
    pub async fn fetch<S, C>(self, mut content: C) -> (C, Self, Result<ElasticRequestFetchRet>)
    where
        C: ElasticContentTrait<S>,
        S: DeserializeOwned,
    {
        let (url, body) = self.get_url_body();

        let fetch_start = SystemTime::now();
        let res = self.client.post(&url).json(&body).send().await;

        let (scroll_id, need_stop, processed_count, fetch_end) = match res {
            Err(err) => {
                let do_retry = match &self.kind {
                    ElasticRequestKind::First { .. } => true,
                    ElasticRequestKind::Next {
                        prev_fetch_start, ..
                    } => {
                        SystemTime::now()
                            .duration_since(*prev_fetch_start)
                            .unwrap()
                            .as_secs()
                            <= self.scroll_timeout
                    }
                };
                if do_retry {
                    warn!(
                        "{}:{}: POST {:?}\n{}\nERROR: {}",
                        file!(),
                        line!(),
                        url,
                        serde_json::to_string_pretty(&body).unwrap(),
                        err
                    );
                    return (content, self, Ok(ElasticRequestFetchRet::Continue));
                } else {
                    let err = anyhow!(
                        "{}:{}: POST {:?}\n{}\nERROR: {}",
                        file!(),
                        line!(),
                        url,
                        serde_json::to_string_pretty(&body).unwrap(),
                        err
                    );
                    error!("{}", err);
                    return (content, self, Err(err));
                }
            }
            Ok(resp) => {
                let status = resp.status();
                if !status.is_success() {
                    if status == reqwest::StatusCode::NOT_FOUND {
                        panic!(
                            "{}:{}: POST {:?}:\n{}\nSTATUS: {}",
                            file!(),
                            line!(),
                            url,
                            serde_json::to_string_pretty(&body).unwrap(),
                            status
                        );
                    }
                    let err = anyhow!(
                        "{}:{}: POST {:?}:\n{}\nSTATUS: {}",
                        file!(),
                        line!(),
                        url,
                        serde_json::to_string_pretty(&body).unwrap(),
                        status
                    );
                    error!("{}", err);
                    return (content, self, Err(err));
                } else {
                    #[derive(Deserialize)]
                    struct Resp<S> {
                        #[serde(rename = "_scroll_id")]
                        scroll_id: String,
                        hits: Hits<S>,
                    }
                    #[derive(Deserialize)]
                    struct Hits<S> {
                        total: usize,
                        hits: Vec<Hit<S>>,
                    }
                    #[derive(Deserialize)]
                    struct Hit<S> {
                        #[serde(rename = "_source")]
                        source: S,
                    }
                    let response_text = match resp.text().await {
                        Ok(value) => value,
                        Err(err) => todo!("{err}"),
                    };
                    let response_json: serde_json::Value =
                        match serde_json::from_str(&response_text) {
                            Ok(value) => value,
                            Err(err) => todo!("{err}"),
                        };
                    let text_pretty = match serde_json::to_string_pretty(&response_json) {
                        Ok(value) => value,
                        Err(err) => todo!("{err}"),
                    };
                    match serde_json::from_str::<Resp<S>>(&text_pretty) {
                        Err(err) => {
                            let err = anyhow!(
                                "{}:{}: POST {:?}\n{}\njson(): {}",
                                file!(),
                                line!(),
                                url,
                                serde_json::to_string_pretty(&body).unwrap(),
                                err
                            );
                            use std::path::PathBuf;
                            let file_path = PathBuf::from("response.pretty.json");

                            use tokio::fs::File;
                            use tokio::io::AsyncWriteExt;
                            let mut file = match File::create(&file_path)
                                .await
                                .context(format!("{file_path:?}"))
                            {
                                Ok(value) => value,
                                Err(err) => todo!("{err}"),
                            };
                            if let Err(err) = file
                                .write_all(text_pretty.as_bytes())
                                .await
                                .context(format!("{file_path:?}"))
                            {
                                todo!("{err}");
                            };
                            panic!("{err}; did write response to {file_path:?}");
                            // return (content, self, Err(err));
                        }
                        Ok(resp) => {
                            let Resp { scroll_id, hits } = resp;
                            let Hits {
                                total: hits_total,
                                hits,
                            } = hits;
                            let processed_count = hits.len()
                                + match &self.kind {
                                    ElasticRequestKind::First { .. } => 0,
                                    ElasticRequestKind::Next {
                                        processed_count, ..
                                    } => *processed_count,
                                };
                            let need_stop = hits.is_empty() || processed_count >= hits_total;
                            let scan_start = match self.kind {
                                ElasticRequestKind::First { .. } => fetch_start,
                                ElasticRequestKind::Next { scan_start, .. } => scan_start,
                            };
                            use chrono::{DateTime, Utc};
                            let duration_millis = (DateTime::<Utc>::from(SystemTime::now())
                                - DateTime::<Utc>::from(scan_start))
                            .num_milliseconds();
                            let speed_k_per_sec = Some(duration_millis)
                                .filter(|duration_millis| duration_millis > &0)
                                .map(|duration_millis| {
                                    processed_count as f64 / duration_millis as f64
                                })
                                .filter(|speed| speed > &0f64);
                            info!(
                                "scanned {processed_count}/{hits_total}{}, elapsed {}{}",
                                speed_k_per_sec
                                    .map(|speed| format!(" ({speed:.2} K/sec)"))
                                    .unwrap_or_else(|| "".to_owned()),
                                arrange_millis::get(duration_millis as u128),
                                speed_k_per_sec
                                    .filter(|_| {
                                        processed_count > 0
                                            && duration_millis > 0
                                            && processed_count < hits_total
                                    })
                                    .map(|speed| {
                                        format!(
                                            ", estimated {}",
                                            arrange_millis::get(
                                                ((hits_total - processed_count) as f64 / speed)
                                                    as u128,
                                            )
                                        )
                                    })
                                    .unwrap_or_else(|| "".to_owned())
                            );
                            trace!(
                                "{}:{}: POST {url:?}\n{}\nneed_stop={need_stop}",
                                file!(),
                                line!(),
                                serde_json::to_string_pretty(&body).unwrap()
                            );
                            content
                                .extend(hits.into_iter().map(|i| i.source).collect(), scan_start);
                            (scroll_id, need_stop, processed_count, SystemTime::now())
                        }
                    }
                }
            }
        };
        let (request, ret) = self.next(
            scroll_id,
            need_stop,
            processed_count,
            fetch_start,
            fetch_end,
        );
        (content, request, Ok(ret))
    }

    // ======================================================================================
    // =================================== private ==========================================

    fn get_url_body(&self) -> (String, Value) {
        match &self.kind {
            ElasticRequestKind::First {
                index_url_part,
                query,
                fields,
                fetch_limit,
                ..
            } => {
                let url = format!(
                    "{}/{}/_search?scroll={}s",
                    self.host, index_url_part, self.scroll_timeout
                );
                let body = json!({
                    "size": fetch_limit,
                    "query": query,
                    "_source": fields,
                });
                (url, body)
            }
            ElasticRequestKind::Next { scroll_id, .. } => {
                let url = format!("{}/_search/scroll", self.host);
                let body = json!({
                    "scroll_id": scroll_id,
                    "scroll": format!("{}s", self.scroll_timeout),
                });
                (url, body)
            }
        }
    }
    fn next(
        self,
        scroll_id: String,
        need_stop: bool,
        processed_count: usize,
        fetch_start: SystemTime,
        fetch_end: SystemTime,
    ) -> (Self, ElasticRequestFetchRet) {
        let Self {
            client,
            host,
            scroll_timeout,
            kind,
            ..
        } = self;
        let scan_start = match kind {
            ElasticRequestKind::First { .. } => fetch_start,
            ElasticRequestKind::Next { scan_start, .. } => scan_start,
        };
        (
            Self {
                client,
                host,
                scroll_timeout,
                kind: ElasticRequestKind::Next {
                    scroll_id,
                    prev_fetch_start: fetch_start,
                    prev_fetch_end: fetch_end,
                    processed_count,
                    scan_start,
                },
            },
            if need_stop {
                ElasticRequestFetchRet::Break
            } else {
                ElasticRequestFetchRet::Continue
            },
        )
    }
}
