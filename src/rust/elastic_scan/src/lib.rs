#[allow(unused_imports)]
use anyhow::{anyhow, bail, Context, Error, Result};

#[allow(unused_imports)]
use log::{debug, error, info, trace, warn};

use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use std::time::{Duration, SystemTime};

mod private;
mod public;
use private::ElasticRequestKind;
pub use public::{ElasticContentTrait, ElasticRequest, ElasticRequestArg, ElasticRequestFetchRet};
