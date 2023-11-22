use crate::cores::releases::CoreReleaseRef;
use crate::params::PagingParams;
use crate::systems::SystemRef;
use crate::teams::TeamRef;
use crate::types::IdOrSlug;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub mod releases;

/// Parameters for filtering the list of cores.
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "rocket", derive(rocket::form::FromForm))]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct CoreListQueryParams<'v> {
    /// Filter cores by supported platform. By default, include all cores.
    #[serde(borrow)]
    pub platform: Option<IdOrSlug<'v>>,

    /// Filter cores by system. By default, include all systems.
    #[serde(borrow)]
    pub system: Option<IdOrSlug<'v>>,

    /// Filter cores by owner team. By default, include all teams.
    #[serde(borrow)]
    pub owner_team: Option<IdOrSlug<'v>>,

    /// Filter by latest release date. By default, include all cores.
    pub release_date_ge: Option<i64>,

    #[serde(flatten)]
    pub paging: PagingParams,
}

impl<'v> CoreListQueryParams<'v> {
    pub fn paging(&self) -> PagingParams {
        self.paging
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct CoreRef {
    pub id: i32,
    pub slug: String,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct CoreListItem {
    pub id: i32,
    pub slug: String,
    pub name: String,
    pub systems: Vec<SystemRef>,
    pub owner_team: TeamRef,
    pub latest_release: Option<CoreReleaseRef>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct CoreCreateRequest<'v> {
    pub slug: &'v str,
    pub name: &'v str,
    pub description: &'v str,
    pub links: BTreeMap<&'v str, &'v str>,
    pub metadata: BTreeMap<&'v str, Value>,
    pub systems: Vec<IdOrSlug<'v>>,
    pub owner_team: IdOrSlug<'v>,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct CoreCreateResponse {
    pub id: i32,
    pub slug: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(schemars::JsonSchema))]
pub struct CoreDetailsResponse {
    pub id: i32,
    pub slug: String,
    pub name: String,
    pub description: String,
    pub links: BTreeMap<String, String>,
    pub metadata: BTreeMap<String, Value>,
    pub systems: Vec<SystemRef>,
    pub owner_team: TeamRef,
}
