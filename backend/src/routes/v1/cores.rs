use crate::db::Db;
use crate::types::FetchModel;
use crate::utils::json;
use crate::{guards, models};
use retronomicon_dto as dto;
use rocket::http::Status;
use rocket::serde::json::Json;
use rocket::{get, post};
use rocket_okapi::openapi;
use serde_json::json;

pub mod releases;

#[openapi(tag = "Cores", ignore = "db")]
#[get("/cores?<filter..>")]
pub async fn cores_list(
    mut db: Db,
    filter: dto::cores::CoreListQueryParams<'_>,
) -> Result<Json<Vec<dto::cores::CoreListItem>>, (Status, String)> {
    let (page, limit) = filter
        .paging()
        .validate()
        .map_err(|e| (Status::BadRequest, e))?;

    let platform = match filter.platform {
        Some(platform) => Some(models::Platform::from_id_or_slug(&mut db, platform).await?),
        None => None,
    };
    let system = match filter.system {
        Some(system) => Some(models::System::from_id_or_slug(&mut db, system).await?),
        None => None,
    };
    let team = match filter.owner_team {
        Some(team) => Some(models::Team::from_id_or_slug(&mut db, team).await?),
        None => None,
    };
    let release = filter
        .release_date_ge
        .and_then(|release| chrono::NaiveDateTime::from_timestamp_opt(release, 0));

    Ok(Json(
        models::Core::list_with_teams_and_releases(
            &mut db,
            page,
            limit,
            platform.as_ref(),
            system.as_ref(),
            team.as_ref(),
            release,
        )
        .await
        .map_err(|e| (Status::InternalServerError, e.to_string()))?
        .into_iter()
        .map(
            |(core, systems, team, core_release, platform)| dto::cores::CoreListItem {
                id: core.id,
                slug: core.slug,
                name: core.name,
                owner_team: team.into(),
                systems: systems.into_iter().map(|s| s.into()).collect(),
                latest_release: core_release.map(|cr| cr.into_ref(platform)),
            },
        )
        .collect(),
    ))
}

#[openapi(tag = "Cores", ignore = "db")]
#[get("/cores/<core_id>")]
pub async fn cores_details(
    mut db: Db,
    core_id: dto::types::IdOrSlug<'_>,
) -> Result<Json<dto::cores::CoreDetailsResponse>, (Status, String)> {
    let (core, owner_team, systems) = models::Core::get_with_owner_and_systems(&mut db, core_id)
        .await
        .map_err(|e| (Status::InternalServerError, e.to_string()))?
        .ok_or((Status::NotFound, "Core not found".to_string()))?;

    Ok(Json(dto::cores::CoreDetailsResponse {
        id: core.id,
        slug: core.slug,
        name: core.name,
        description: core.description,
        metadata: json::metadata_into_btree_map(core.metadata)
            .map_err(|e| (Status::InternalServerError, e.to_string()))?,
        links: json::links_into_btree_map(core.links)
            .map_err(|e| (Status::InternalServerError, e.to_string()))?,
        systems: systems.into_iter().map(|s| s.into()).collect(),
        owner_team: owner_team.into(),
    }))
}

#[openapi(tag = "Cores", ignore = "db")]
#[post("/cores", format = "application/json", data = "<form>")]
pub async fn cores_create(
    mut db: Db,
    user: guards::users::AuthenticatedUserGuard,
    form: Json<dto::cores::CoreCreateRequest<'_>>,
) -> Result<Json<dto::cores::CoreCreateResponse>, (Status, String)> {
    let dto::cores::CoreCreateRequest {
        slug,
        name,
        description,
        metadata,
        links,
        systems,
        owner_team,
    } = form.into_inner();

    let mut systems_vec = Vec::with_capacity(systems.len());
    for s in systems.into_iter() {
        let system = models::System::from_id_or_slug(&mut db, s).await?;
        systems_vec.push(system);
    }
    let (_user, team, role) =
        models::User::get_user_team_and_role(&mut db, user.into(), owner_team)
            .await
            .map_err(|e| (Status::InternalServerError, e.to_string()))?
            .ok_or((Status::NotFound, "Not found".to_string()))?;

    if !role.can_create_cores() {
        return Err((Status::Forbidden, "User cannot create cores".to_string()));
    }

    let core = models::Core::create(
        &mut db,
        slug,
        name,
        description,
        json!(links),
        json!(metadata),
        systems_vec.as_slice(),
        &team,
    )
    .await
    .map_err(|e| (Status::InternalServerError, e.to_string()))?;

    Ok(Json(dto::cores::CoreCreateResponse {
        id: core.id,
        slug: core.slug,
    }))
}
