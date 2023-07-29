#[allow(unused_imports)]
use anyhow::{anyhow, bail, Context, Error, Result};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

// https://docs.rs/built/0.5.1/built/index.html
pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
use built_info::*;
use common_macros2::*;
use serde::{Deserialize, Serialize};
use structopt::StructOpt;

mod common;
mod pasitos;
// mod server;
use common::*;

#[tokio::main]
async fn main() -> Result<()> {
    main_helper().await
}

// ==================================================
// ==================================================

declare_env_settings_for_server! {
    settings_toml_path: std::path::PathBuf,
}

declare_settings! {
    elastic: ElasticSettings,
    store: StoreSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElasticSettings {
    max_at_once: usize,
    host: String,
    fetch_once: Option<bool>,
    fetch_limit: usize,
    scroll_timeout_secs: u64,
    error_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreSettings {
    bunches_to_save_max_len: usize,
}

#[derive(Debug, Clone, StructOpt)]
pub enum Command {
    ForSite {
        /// Optional: 'habit' or 'cottage', if none is specified both will be used
        facet: Option<ForSiteFacet>,
    },
    ForAnalytics {},
}
#[derive(
    Debug, strum::EnumIter, Clone, Copy, strum::Display, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
pub enum ForSiteFacet {
    Habit,
    Cottage,
}
common_macros2::r#impl!(FromStr for ForSiteFacet; strum);

common_macros2::impl_from!(ForSiteFacet => MlsFacet, from: From, match from {
    From::Habit => Self::MskHabitSale,
    From::Cottage => Self::MskCottage,
});

#[derive(Debug, Clone, Copy)]
pub enum ExportFor {
    Site(ForSiteFacet),
    Analytics,
}

common_macros2::impl_from!(ExportFor => MlsFacet, from: From, match from {
    From::Site(facet) => facet.into(),
    From::Analytics => Self::MskHabitSale,
});

mod elastic_scan_specific;

use elastic_scan::{ElasticRequest, ElasticRequestFetchRet};

use mls_facet::*;

use std::path::PathBuf;

mod declare_fn_get_last_but_one_and_last_extension_file_path;
use declare_fn_get_last_but_one_and_last_extension_file_path::*;

// ==================================================
// ==================================================
