use crate::db::Db;
use crate::models::{Platform, System, Team};
use crate::types::FromIdOrSlug;
use crate::{models, schema};
use diesel::prelude::*;
use diesel::{AsExpression, FromSqlRow, Identifiable, Queryable};
use retronomicon_dto as dto;
use rocket_db_pools::diesel::{AsyncConnection, RunQueryDsl};
use serde_json::Value as Json;

mod releases;
pub use releases::*;

#[derive(Identifiable, Selectable, Queryable, Associations, Debug)]
#[diesel(primary_key(core_id, system_id))]
#[diesel(belongs_to(models::Core))]
#[diesel(belongs_to(models::System))]
#[diesel(table_name = schema::core_systems)]
pub struct CoreSystems {
    pub core_id: i32,
    pub system_id: i32,
}

#[derive(Queryable, Debug, Identifiable, Selectable)]
#[diesel(table_name = schema::cores)]
pub struct Core {
    pub id: i32,
    pub slug: String,
    pub name: String,
    pub description: String,
    pub metadata: Json,
    pub links: Json,
    pub owner_team_id: i32,
}

#[rocket::async_trait]
impl FromIdOrSlug for Core {
    async fn from_id(db: &mut Db, id: i32) -> Result<Option<Self>, diesel::result::Error>
    where
        Self: Sized,
    {
        schema::cores::table
            .filter(schema::cores::id.eq(id))
            .first::<Self>(db)
            .await
            .optional()
    }

    async fn from_slug(db: &mut Db, slug: &str) -> Result<Option<Self>, diesel::result::Error>
    where
        Self: Sized,
    {
        schema::cores::table
            .filter(schema::cores::slug.eq(slug))
            .first::<Self>(db)
            .await
            .optional()
    }
}

impl Core {
    pub async fn list(
        db: &mut Db,
        page: i64,
        limit: i64,
    ) -> Result<Vec<Self>, diesel::result::Error> {
        schema::cores::table
            .offset(page * limit)
            .limit(limit)
            .load::<Self>(db)
            .await
    }

    pub async fn list_with_teams(
        db: &mut Db,
        page: i64,
        limit: i64,
    ) -> Result<Vec<(Self, models::Team)>, diesel::result::Error> {
        schema::cores::table
            .inner_join(schema::teams::table)
            .offset(page * limit)
            .limit(limit)
            .load::<(Self, models::Team)>(db)
            .await
    }

    pub async fn list_with_teams_and_releases(
        db: &mut Db,
        page: i64,
        limit: i64,
        platform: Option<&Platform>,
        system: Option<&System>,
        team: Option<&Team>,
        release_date_ge: Option<chrono::NaiveDateTime>,
    ) -> Result<
        Vec<(
            Self,
            Vec<models::System>,
            models::Team,
            Option<models::CoreRelease>,
            models::Platform,
        )>,
        diesel::result::Error,
    > {
        let mut query = schema::cores::table
            .inner_join(schema::teams::table)
            .left_join(
                schema::core_releases::table.on(schema::core_releases::id.eq(
                    // Diesel does not support subqueries on joins, so we have to use raw SQL.
                    // It's okay because it does not actually need inputs.
                    diesel::dsl::sql(
                        r#"(
                        SELECT id FROM core_releases
                            WHERE cores.id = core_releases.core_id
                            ORDER BY date_released DESC, id DESC
                            LIMIT 1
                        )"#,
                    ),
                )),
            )
            .inner_join(
                schema::platforms::table
                    .on(schema::platforms::id.eq(schema::core_releases::platform_id)),
            )
            .inner_join(schema::core_systems::table)
            .inner_join(
                schema::systems::table.on(schema::systems::id.eq(schema::core_systems::system_id)),
            )
            .into_boxed();

        if let Some(platform) = platform {
            query = query.filter(schema::platforms::id.eq(platform.id));
        }

        if let Some(system) = system {
            query = query.filter(schema::systems::id.eq(system.id));
        }

        if let Some(team) = team {
            query = query.filter(schema::teams::id.eq(team.id));
        }

        if let Some(release_date_ge) = release_date_ge {
            query = query.filter(schema::core_releases::date_released.ge(release_date_ge));
        }

        let cores = query
            .select((
                schema::cores::all_columns,
                schema::teams::all_columns,
                Option::<models::CoreRelease>::as_select(),
                schema::platforms::all_columns,
            ))
            .offset(page * limit)
            .limit(limit)
            .load::<(
                Self,
                models::Team,
                Option<models::CoreRelease>,
                models::Platform,
            )>(db)
            .await?;

        let all_cores = schema::cores::table
            .select(Core::as_select())
            .load(db)
            .await?;

        let systems = CoreSystems::belonging_to(&cores.iter().map(|r| r.0).collect::<Vec<_>>())
            .inner_join(schema::systems::table)
            .select((CoreSystems::as_select(), System::as_select()))
            .load(db)
            .await?;

        let cores_with_systems: Vec<(
            Self,
            Vec<models::System>,
            models::Team,
            Option<models::CoreRelease>,
            models::Platform,
        )> = systems
            .grouped_by(&all_cores)
            .into_iter()
            .zip(cores)
            .map(|(systems, core)| (core.0, systems, core.1, core.2, core.3))
            .collect::<Vec<_>>();

        Ok(cores_with_systems)
    }

    pub async fn create(
        db: &mut Db,
        slug: &str,
        name: &str,
        description: &str,
        metadata: Json,
        links: Json,
        systems: &[models::System],
        owner_team: &models::Team,
    ) -> Result<Self, diesel::result::Error> {
        let result = diesel::insert_into(schema::cores::table)
            .values((
                schema::cores::slug.eq(slug),
                schema::cores::name.eq(name),
                schema::cores::description.eq(description),
                schema::cores::metadata.eq(metadata),
                schema::cores::links.eq(links),
                schema::cores::owner_team_id.eq(owner_team.id),
            ))
            .returning(schema::cores::all_columns)
            .get_result::<Self>(db)
            .await?;
        let system_ids = systems.iter().map(|s| s.id).collect::<Vec<_>>();
        diesel::insert_into(schema::core_systems::table)
            .values(
                system_ids
                    .iter()
                    .map(|&system_id| {
                        (
                            schema::core_systems::core_id.eq(result.id),
                            schema::core_systems::system_id.eq(system_id),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .execute(db)
            .await?;
        Ok(result)
    }

    pub async fn get_with_owner_and_systems(
        db: &mut Db,
        id: dto::types::IdOrSlug<'_>,
    ) -> Result<Option<(Self, models::Team, Vec<models::System>)>, diesel::result::Error> {
        let mut query = schema::cores::table
            .inner_join(schema::teams::table)
            .into_boxed();

        if let Some(id) = id.as_id() {
            query = query.filter(schema::cores::id.eq(id));
        } else if let Some(slug) = id.as_slug() {
            query = query.filter(schema::cores::slug.eq(slug));
        } else {
            return Ok(None);
        }

        let results = query.first::<(Self, models::Team)>(db).await.optional()?;
        let results = if let Some(r) = results {
            r
        } else {
            return Ok(None);
        };

        let systems = schema::core_systems::table
            .inner_join(schema::systems::table)
            .filter(schema::core_systems::core_id.eq(results.0.id))
            .select(schema::systems::all_columns)
            .load::<models::System>(db)
            .await?;

        Ok(Some((results.0, results.1, systems)))
    }
}
