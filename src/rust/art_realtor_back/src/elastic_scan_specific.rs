#[allow(unused_imports)]
use anyhow::{anyhow, bail, Context, Error, Result};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use super::*;
use std::string::ToString;

pub use elastic_scan::{
    ElasticContentTrait, ElasticRequest, ElasticRequestArg, ElasticRequestFetchRet,
};

use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use std::io::Write;
pub type ElasticSource = serde_json::Value;
pub struct ElasticContentSharedInner {
    mode: ExportFor, // Option due to
    did_write_anything: bool,
    pub bunches_to_save: VecDeque<OffersToSave>,
    pub need_finish: bool,
    specific: Option<ElasticContentSharedInnerSpecific>,
}
use std::collections::VecDeque;
type OffersToSave = Vec<ElasticSource>;
pub enum ElasticContentSharedInnerSpecific {
    Json(Arc<RwLock<Option<JsonSpecific>>>),
    Csv {
        offer: Arc<RwLock<Option<CsvSpecific>>>,
        offer_pub_list: Arc<RwLock<Option<CsvSpecific>>>,
    },
}
pub struct JsonSpecific {
    encoder: GzEncoder<std::fs::File>,
    temp_filepath: PathBuf,
    output_filepath: PathBuf,
}
pub struct CsvSpecific {
    writer: CsvWriter,
    fields: Vec<String>,
    temp_filepath: PathBuf,
    output_filepath: PathBuf,
}
type CsvWriter = csv::Writer<GzEncoder<std::fs::File>>;

use flate2::write::GzEncoder;
use flate2::Compression;

declare_fn_get_last_but_one_and_last_extension_file_path!(.csv.gz);
declare_fn_get_last_but_one_and_last_extension_file_path!(.json.gz);

impl ElasticContentSharedInner {
    pub fn new(mode: ExportFor) -> Result<Self> {
        let specific = match mode {
            ExportFor::Site(facet) => {
                let facet = MlsFacet::from(facet);
                let output_filepath = (*PARAMS.read().unwrap())
                    .as_ref()
                    .unwrap()
                    .run_dir
                    .clone()
                    .join(std::path::Path::new(&format!(
                        "for-site.{}",
                        facet.to_string().to_lowercase()
                    )));
                let output_filepath = get_json_gz_filepath(output_filepath);
                let temp_filepath =
                    output_filepath
                        .parent()
                        .unwrap()
                        .join(std::path::Path::new(&{
                            let mut ret = std::ffi::OsString::from(".");
                            ret.push(output_filepath.file_name().unwrap());
                            ret
                        }));
                let file = std::fs::File::create(&temp_filepath)?;
                let mut encoder = GzEncoder::new(file, Compression::default());
                encoder
                    .write_all("[\n".as_bytes())
                    .map_err(|err| anyhow!("{}:{}: {err}", file!(), line!()))?;
                ElasticContentSharedInnerSpecific::Json(Arc::new(RwLock::new(Some(JsonSpecific {
                    encoder,
                    temp_filepath,
                    output_filepath,
                }))))
            }
            ExportFor::Analytics => {
                let facet = MlsFacet::MskHabitSale;
                let offer = {
                    let output_filepath = (*PARAMS.read().unwrap())
                        .as_ref()
                        .unwrap()
                        .run_dir
                        .clone()
                        .join(std::path::Path::new(&format!(
                            "for-analytics.{}.offer",
                            facet.to_string().to_lowercase()
                        )));
                    let output_filepath = get_csv_gz_filepath(output_filepath);
                    let temp_filepath =
                        output_filepath
                            .parent()
                            .unwrap()
                            .join(std::path::Path::new(&{
                                let mut ret = std::ffi::OsString::from(".");
                                ret.push(output_filepath.file_name().unwrap());
                                ret
                            }));

                    let file = std::fs::File::create(&temp_filepath)?;
                    let encoder = GzEncoder::new(file, Compression::default());
                    let mut writer = csv::Writer::from_writer(encoder);
                    let fields = vec![
                        // ----------
                        "guid",
                        "sale_type_name",
                        "note",
                        "built_year",
                        "building_batch_name",
                        "balcony_type_name",
                        "water_closet_type_name",
                        "parking_type_name",
                        "territory_type_name",
                        "window_overlook_type_name",
                        "apartment_condition_type_name",
                        "elevator_type_name",
                        "walls_material_type_name",
                        "realty_type_name",
                        // === ,
                        "offer_part_count",
                        "offer_room_count",
                        "total_part_count",
                        "total_room_count",
                        "is_studio",
                        "is_apartment",
                        "is_free_planning",
                        // === ,
                        "total_square",
                        "kitchen_square",
                        "life_square",
                        // === ,
                        "storey",
                        "storeys_count",
                        // ===,
                        "geo_cache_region_name",
                        "geo_cache_town_name",
                        "geo_cache_settlement_name",
                        "geo_cache_estate_object_name",
                        "geo_town_transport_access",
                        "geo_cache_micro_district_name",
                        "geo_cache_street_address_segment",
                        "geo_building",
                        "geo_cache_building_in_renovation",
                        // === ,
                        "geo_cache_subway_station_name_1",
                        "walking_access_1",
                        "transport_access_1",
                        // === ,
                        "price_rub",
                        "location.lat",
                        "location.lon",
                    ]
                    .into_iter()
                    .map(|s| s.to_owned())
                    .collect();
                    writer.write_record(&fields)?;
                    Arc::new(RwLock::new(Some(CsvSpecific {
                        writer,
                        fields,
                        temp_filepath,
                        output_filepath,
                    })))
                };
                let offer_pub_list = {
                    let output_filepath = (*PARAMS.read().unwrap())
                        .as_ref()
                        .unwrap()
                        .run_dir
                        .clone()
                        .join(std::path::Path::new(&format!(
                            "for-analytics.{}.offer_pub_list",
                            facet.to_string().to_lowercase()
                        )));
                    let output_filepath = get_csv_gz_filepath(output_filepath);
                    let temp_filepath =
                        output_filepath
                            .parent()
                            .unwrap()
                            .join(std::path::Path::new(&{
                                let mut ret = std::ffi::OsString::from(".");
                                ret.push(output_filepath.file_name().unwrap());
                                ret
                            }));

                    let file = std::fs::File::create(&temp_filepath)?;
                    let encoder = GzEncoder::new(file, Compression::default());
                    let mut writer = csv::Writer::from_writer(encoder);
                    let fields: Vec<&str> = vec![
                        "start_pub_datetime",
                        "end_pub_datetime",
                        "price",
                        "deal_status_id",
                    ];
                    writer.write_record(vec!["guid"].into_iter().chain(fields.to_vec()))?;
                    let fields = fields.into_iter().map(|s| s.to_owned()).collect();
                    Arc::new(RwLock::new(Some(CsvSpecific {
                        writer,
                        fields,
                        temp_filepath,
                        output_filepath,
                    })))
                };

                ElasticContentSharedInnerSpecific::Csv {
                    offer,
                    offer_pub_list,
                }
            }
        };

        Ok(Self {
            specific: Some(specific),
            did_write_anything: false,
            bunches_to_save: VecDeque::new(),
            need_finish: false,
            mode,
        })
    }
    pub fn finish(&mut self) -> Result<()> {
        // use std::path::PathBuf;
        struct RenameTask {
            from: PathBuf,
            to: PathBuf,
        }
        let rename_tasks: Vec<RenameTask> = {
            let specific = self
                .specific
                .take()
                .ok_or_else(|| anyhow!("{}:{}: specific is none", file!(), line!()))?;
            match specific {
                ElasticContentSharedInnerSpecific::Csv {
                    offer,
                    offer_pub_list,
                } => {
                    if let (
                        Some(CsvSpecific {
                            writer: mut offer_writer,
                            temp_filepath: offer_temp_filepath,
                            output_filepath: offer_output_filepath,
                            ..
                        }),
                        Some(CsvSpecific {
                            writer: mut offer_pub_list_writer,
                            temp_filepath: offer_pub_list_temp_filepath,
                            output_filepath: offer_pub_list_output_filepath,
                            ..
                        }),
                    ) = (
                        (*offer.write().unwrap()).take(),
                        (*offer_pub_list.write().unwrap()).take(),
                    ) {
                        offer_writer.flush()?;
                        offer_pub_list_writer.flush()?;
                        vec![
                            RenameTask {
                                from: offer_temp_filepath,
                                to: offer_output_filepath,
                            },
                            RenameTask {
                                from: offer_pub_list_temp_filepath,
                                to: offer_pub_list_output_filepath,
                            },
                        ]
                    } else {
                        vec![]
                    }
                }
                ElasticContentSharedInnerSpecific::Json(specific) => {
                    if let Some(JsonSpecific {
                        mut encoder,
                        temp_filepath,
                        output_filepath,
                    }) = (*specific.write().unwrap()).take()
                    {
                        encoder.write_all("] ".as_bytes()).unwrap();
                        encoder
                            .finish()
                            .map_err(|err| anyhow!("{}:{}: {err}", file!(), line!()))?;
                        vec![RenameTask {
                            from: temp_filepath,
                            to: output_filepath,
                        }]
                    } else {
                        vec![]
                    }
                }
            }
        };
        for RenameTask { from, to } in rename_tasks.iter() {
            std::fs::rename(from, to)
                .map_err(|err| anyhow!("Failed to rename {from:?} to {to:?}: {err}"))?;
        }
        println!(
            "result saved to {}",
            rename_tasks
                .into_iter()
                .map(|RenameTask { to, .. }| format!("{to:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        // for
        Ok(())
    }
}

pub type ElasticContentShared = Arc<RwLock<ElasticContentSharedInner>>;
pub struct ElasticContent {
    pub index: MlsFacet,
    pub shared: ElasticContentShared,
}

impl ElasticContent {
    pub fn new(index: MlsFacet, shared: ElasticContentShared) -> Self {
        Self { index, shared }
    }
    pub fn new_request(&self, mode: ExportFor) -> ElasticRequest {
        ElasticRequest::new(ElasticRequestArg {
            host: settings!(elastic.host).clone(),
            index_url_part: self.index.elastic_index(),
            query: match mode {
                ExportFor::Analytics => serde_json::json!({
                  "bool": {
                    "must": [
                      {
                        "range": {
                          "pub_datetime": {
                              "gte": "now-90d/d" /* за последние 3 месяца */
                          }
                        }
                      },
                      {
                          "term": {
                              "deal_status_id": 3 /* Только снятые с продажи */
                          }
                      } ,
                      {
                          "terms": {
                              "media_id": [
                                  3 /* WinNER - зелёная зона */,
                                  24 /* WinNER - белая зона */,
                                  17 /* Cian */,
                              ]
                          }
                      }
                    ]
                  }
                }),
                ExportFor::Site(ForSiteFacet::Cottage) => serde_json::json!({
                  "bool": {
                    "must": [
                      {
                        "range": {
                          "pub_datetime": {
                              "gte": "now-2d/d" /* за последние 2 дня */
                          }
                        }
                      },
                      {
                          "term": {
                              "deal_type_id": 1 /* Только продажа */
                          }
                      } ,
                      {
                          "term": {
                              "deal_status_id": 1 /* Только актуальные */
                          }
                      } ,
                      {
                          "terms": {
                              "media_id": [
                                  3 /* WinNER - зелёная зона */,
                                  24 /* WinNER - белая зона */,
                                  15 /* Sob.ru */,
                                  17 /* Cian */ ,
                                  21 /* Avito */,
                                  23 /* Яндекс */,
                                  30 /* radver.ru */,
                                  31 /* afy.ru */,
                                  32 /* Домклик */,
                                  // 33 /* Прочие (Д) */
                              ]
                          }
                      }
                    ]
                  }
                }),
                ExportFor::Site(ForSiteFacet::Habit) => serde_json::json!({
                  "bool": {
                    "must": [
                      {
                        "range": {
                          "pub_datetime": {
                              "gte": "now-2d/d" /* за последние 2 дня */
                          }
                        }
                      },
                      {
                          "term": {
                              "deal_status_id": 1 /* Только актуальные */
                          }
                      } ,
                      {
                          "terms": {
                              "media_id": [
                                  3 /* WinNER - зелёная зона */,
                                  24 /* WinNER - белая зона */,
                                  15 /* Sob.ru */,
                                  17 /* Cian */ ,
                                  21 /* Avito */,
                                  23 /* Яндекс */,
                                  30 /* radver.ru */,
                                  31 /* afy.ru */,
                                  32 /* Домклик */,
                                  // 33 /* Прочие (Д) */
                              ]
                          }
                      }
                    ]
                  }
                }),
            },
            fields: self.fields(),
            fetch_limit: settings!(elastic.fetch_limit),
            scroll_timeout: settings!(elastic.scroll_timeout_secs),
        })
    }
}

impl ElasticContentSharedInner {
    pub fn save(&mut self) -> Result<()> {
        if let Some(offers_to_save) = self.bunches_to_save.pop_front() {
            match self.specific.as_mut().unwrap() {
                ElasticContentSharedInnerSpecific::Json(specific) => {
                    for (i, offer) in offers_to_save.into_iter().enumerate() {
                        // use json::{By, Json, JsonSource};
                        let offer = Json::new(offer, JsonSource::Name(i.to_string()));
                        let can_export = match self.mode {
                            ExportFor::Analytics => unreachable!(),
                            ExportFor::Site(ForSiteFacet::Cottage) => true,
                            ExportFor::Site(ForSiteFacet::Habit) => {
                                // is_whole_flat
                                if offer
                                    .get([By::key("total_part_count")])
                                    .unwrap()
                                    .as_null()
                                    .is_ok()
                                {
                                    if offer
                                        .get([By::key("total_room_count")])
                                        .and_then(|value| value.as_u8())
                                        .map(|value| value > 0)
                                        .unwrap_or(false)
                                    {
                                        let value =
                                            offer.get([By::key("offer_room_count")]).unwrap();
                                        value.as_null().is_ok()
                                            || value.as_u8().map(|value| value > 0).unwrap_or(false)
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            }
                        };
                        if can_export {
                            let mut offer = offer.value;
                            if let serde_json::Value::Object(ref mut map) = &mut offer {
                                map.remove("offer_room_count");
                                map.remove("total_part_count");
                            }
                            (*specific.write().unwrap())
                                .as_mut()
                                .unwrap()
                                .encoder
                                .write_all(
                                    format!(
                                        "{}{}\n",
                                        if self.did_write_anything { "," } else { "" },
                                        serde_json::to_string(&offer).unwrap()
                                    )
                                    .as_bytes(),
                                )
                                .map_err(|err| anyhow!("{}:{}: {err}", file!(), line!()))?;
                            self.did_write_anything = true;
                        }
                    }
                }
                ElasticContentSharedInnerSpecific::Csv {
                    offer: offer_specific,
                    offer_pub_list: offer_pub_list_specific,
                    ..
                } => {
                    let offer_specific = &mut *offer_specific.write().unwrap();
                    let offer_specific = &mut offer_specific.as_mut().unwrap();
                    let offer_fields = &offer_specific.fields;

                    let offer_pub_list_specific = &mut *offer_pub_list_specific.write().unwrap();
                    let offer_pub_list_specific = &mut offer_pub_list_specific.as_mut().unwrap();
                    for offer in offers_to_save.into_iter() {
                        let mut record: Vec<String> = vec![];
                        let offer = serde_json::to_value(offer).unwrap();
                        let offer = Json::new(offer, JsonSource::Name("offer".to_owned()));
                        if let Ok(guid) = offer
                            .get([By::key("guid")])
                            .and_then(|value| value.as_string(false))
                        {
                            if let Ok(offer_pub_list) = offer.get([By::key("offer_pub_list")]) {
                                if let Ok(iter) = offer_pub_list.iter_vec() {
                                    struct Record {
                                        start_pub_datetime: String,
                                        end_pub_datetime: String,
                                        price: String,
                                        deal_status_id: String,
                                    }
                                    let mut curr_opt: Option<Record> = None;
                                    for offer_pub_list_item in iter {
                                        enum Set {
                                            StartPubDatetime(String),
                                            Whole(Record),
                                        }

                                        let next = Record {
                                            deal_status_id: field_value(
                                                &offer_pub_list_item,
                                                "deal_status_id",
                                            ),
                                            start_pub_datetime: field_value(
                                                &offer_pub_list_item,
                                                "start_pub_datetime",
                                            ),
                                            end_pub_datetime: field_value(
                                                &offer_pub_list_item,
                                                "end_pub_datetime",
                                            ),
                                            price: field_value(&offer_pub_list_item, "price"),
                                        };
                                        let set = if let Some(curr) = curr_opt.as_ref() {
                                            if curr.deal_status_id == next.deal_status_id
                                                && curr.price == next.price
                                                && curr.start_pub_datetime <= next.end_pub_datetime
                                            {
                                                Some(Set::StartPubDatetime(next.start_pub_datetime))
                                            } else {
                                                write_curr(
                                                    curr,
                                                    &guid,
                                                    &mut offer_pub_list_specific.writer,
                                                )?;
                                                Some(Set::Whole(next))
                                            }
                                        } else {
                                            Some(Set::Whole(next))
                                        };
                                        match set {
                                            None => {}
                                            Some(Set::Whole(next)) => curr_opt = Some(next),
                                            Some(Set::StartPubDatetime(
                                                next_start_pub_datetime,
                                            )) => {
                                                curr_opt.as_mut().unwrap().start_pub_datetime =
                                                    next_start_pub_datetime
                                            }
                                        }
                                        // }
                                    }
                                    fn write_curr(
                                        curr: &Record,
                                        guid: &str,
                                        writer: &mut CsvWriter,
                                    ) -> Result<()> {
                                        let record: Vec<&str> = vec![
                                            guid,
                                            &curr.start_pub_datetime,
                                            &curr.end_pub_datetime,
                                            &curr.price,
                                            &curr.deal_status_id,
                                        ];
                                        writer.write_record(&record)?;
                                        Ok(())
                                    }
                                    if let Some(curr) = curr_opt {
                                        write_curr(
                                            &curr,
                                            &guid,
                                            &mut offer_pub_list_specific.writer,
                                        )?;
                                    }
                                }
                            }
                        }
                        for field_name in offer_fields.iter() {
                            record.push(field_value(&offer, field_name));
                        }
                        offer_specific.writer.write_record(&record)?;
                    }
                }
            }
        }
        Ok(())
    }
}
impl ElasticContentTrait<ElasticSource> for ElasticContent {
    fn extend(&mut self, source: Vec<ElasticSource>, _scan_start: SystemTime) {
        self.shared
            .write()
            .unwrap()
            .bunches_to_save
            .push_back(source);
    }
    fn fields(&self) -> Vec<String> {
        let mode = self.shared.read().unwrap().mode;
        match mode {
            ExportFor::Analytics => {
                let shared = &self.shared.read().unwrap();
                let ElasticContentSharedInnerSpecific::Csv {
                    offer,
                    ..
                } = shared.specific.as_ref().unwrap() else { unreachable!(); };
                let offer = &*offer.read().unwrap();
                let offer_fields = &offer.as_ref().unwrap().fields;
                offer_fields
                    .clone()
                    .iter()
                    .filter_map(|field_name| field_name.split('.').next().map(|s| s.to_owned()))
                    // .filter(|s| !matches!(s.as_str(), "lat" | "lon"))
                    .chain(vec!["offer_pub_list"].into_iter().map(|s| s.to_owned()))
                    .collect()
            }
            ExportFor::Site(ForSiteFacet::Cottage) => vec![
                "guid",
                "update_datetime",
                "pub_datetime",
                "geo_cache_country_name",
                "geo_cache_state_name",
                "geo_cache_settlement_name",
                "geo_cache_town_name_2",
                "geo_cache_estate_object_name",
                "geo_cache_district_name",
                "geo_cache_region_name",
                "geo_cache_highway_name_1",
                "geo_cache_street_name",
                "geo_cache_building_name",
                // ----------
                "geo_town_transport_access",
                "price_rub",
                "land_square",
                "house_square",
                "storeys_count",
                "gas_type_name",
                "walls_material_type_name",
                "electricity_type_name",
                "plumbing_type_name",
                "sewerage_type_name",
                "realty_type_name",
                "land_category_name",
                "photo_list",
                "media_name",
                "note",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
            ExportFor::Site(ForSiteFacet::Habit) => vec![
                "guid",
                "update_datetime",
                "pub_datetime",
                "geo_cache_country_name",
                "geo_cache_state_name",
                "geo_cache_settlement_name",
                "geo_cache_town_name_2",
                "geo_cache_estate_object_name",
                "geo_cache_district_name",
                "geo_cache_region_name",
                "geo_cache_highway_name_1",
                "geo_cache_street_name",
                "geo_cache_building_name",
                "geo_cache_subway_station_name_1",
                "geo_cache_subway_station_name_2",
                "geo_cache_subway_station_name_3",
                "geo_cache_subway_station_name_4",
                "walking_access_1",
                "walking_access_2",
                "walking_access_3",
                "walking_access_4",
                "transport_access_1",
                "transport_access_2",
                "transport_access_3",
                "transport_access_4",
                "price_rub",
                "total_part_count",
                "offer_room_count",
                "total_room_count",
                "is_studio",
                "is_apartment",
                "is_free_planning",
                "total_square",
                "kitchen_square",
                "life_square",
                "photo_list",
                "media_name",
                "note",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect(),
        }
    }
}

const USE_COMMA_FOR_FLOATS: bool = false;
use json::{By, Json, JsonSource};
fn field_value(container: &Json, field_name: &str) -> String {
    container
        .get(field_name.split('.').map(By::key).collect::<Vec<_>>())
        .ok()
        .and_then(|value| value.as_string(false).ok())
        .and_then(|value| {
            if field_name.ends_with("_datetime") {
                chrono::DateTime::parse_from_rfc3339(&value)
                    .ok()
                    .map(|datetime| {
                        datetime
                            .with_timezone(&chrono_tz::Europe::Moscow)
                            .format("%F") // https://docs.rs/chrono/latest/chrono/format/strftime/index.html
                            .to_string()
                    })
            } else {
                Some(value)
            }
        })
        .map(|value| {
            if !USE_COMMA_FOR_FLOATS {
                value
            } else {
                match value.parse::<f64>() {
                    Err(_err) => value,
                    Ok(value) => {
                        if value.fract() == 0f64 {
                            (value as i64).to_string()
                        } else {
                            value
                                .to_string()
                                .chars()
                                .map(|ch| match ch {
                                    '.' => ',',
                                    _ => ch,
                                })
                                .collect()
                        }
                    }
                }
            }
        })
        .map(|value| {
            if value.starts_with('=') {
                format!("'{}", value)
            } else {
                value
            }
        })
        .unwrap_or_else(|| "".to_owned())
}
