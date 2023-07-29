#[allow(unused_imports)]
use anyhow::{anyhow, bail, Context, Error, Result};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

// use common_macros;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, strum::EnumIter, Clone, Copy, strum::Display, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
pub enum MlsFacet {
    // #[serde(rename_all = "snake_case")], it does not work as expected
    #[serde(rename = "msk_habit")]
    #[strum(serialize = "msk_habit")]
    MskHabitSale,
    #[serde(rename = "msk_rent_habit")]
    #[strum(serialize = "msk_rent_habit")]
    MskHabitRent,
    #[serde(rename = "msk_cottage")]
    #[strum(serialize = "msk_cottage")]
    MskCottage,
    #[serde(rename = "msk_commre")]
    #[strum(serialize = "msk_commre")]
    MskCommre,
    #[serde(rename = "msk_stall")]
    #[strum(serialize = "msk_stall")]
    MskStall,
    #[serde(rename = "msk_new_building")]
    #[strum(serialize = "msk_new_building")]
    MskNewBuildingSale,
    #[serde(rename = "krasnodar_habit")]
    #[strum(serialize = "krasnodar_habit")]
    KrasnodarHabitSale,
    #[serde(rename = "rgn_habit")]
    #[strum(serialize = "rgn_habit")]
    RgnHabitSale,
}
common_macros2::r#impl!(FromStr for MlsFacet; strum);

impl MlsFacet {
    pub fn table_name(&self) -> String {
        format!("{self}_adv")
    }
    pub fn elastic_index(&self) -> String {
        format!("{self}_advs")
    }
}
