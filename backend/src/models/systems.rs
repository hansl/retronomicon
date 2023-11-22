use crate::db::Db;
use crate::types::FromIdOrSlug;
use crate::{models, schema};
use diesel::prelude::*;
use diesel::{Identifiable, Queryable};
use retronomicon_dto as dto;
use retronomicon_dto::types::IdOrSlug;
use rocket_db_pools::diesel::{AsyncConnection, RunQueryDsl};
use serde_json::Value as Json;
use std::collections::BTreeMap;

#[derive(Queryable, Debug, Identifiable, Selectable)]
#[diesel(table_name = schema::systems)]
#[diesel(belongs_to(models::Team))]
pub struct System {
    pub id: i32,
    pub slug: String,
    pub name: String,
    pub description: String,
    pub manufacturer: String,
    pub links: Json,
    pub metadata: Json,
    pub owner_team_id: i32,
}

#[rocket::async_trait]
impl FromIdOrSlug for System {
    async fn from_id(db: &mut Db, id: i32) -> Result<Option<Self>, diesel::result::Error> {
        schema::systems::table
            .filter(schema::systems::id.eq(id))
            .first::<Self>(db)
            .await
            .optional()
    }

    async fn from_slug(db: &mut Db, slug: &str) -> Result<Option<Self>, diesel::result::Error> {
        schema::systems::table
            .filter(schema::systems::slug.eq(slug))
            .first::<Self>(db)
            .await
            .optional()
    }
}

impl From<System> for dto::systems::SystemRef {
    fn from(value: System) -> Self {
        Self {
            id: value.id,
            slug: value.slug,
        }
    }
}

impl System {
    pub async fn list(
        db: &mut Db,
        page: i64,
        limit: i64,
    ) -> Result<Vec<Self>, diesel::result::Error> {
        schema::systems::table
            .offset(page * limit)
            .limit(limit)
            .load::<Self>(db)
            .await
    }

    pub async fn list_with_team(
        db: &mut Db,
        page: i64,
        limit: i64,
    ) -> Result<Vec<(Self, models::Team)>, diesel::result::Error> {
        schema::systems::table
            .offset(page * limit)
            .limit(limit)
            .inner_join(schema::teams::table)
            .load::<(Self, models::Team)>(db)
            .await
    }

    pub async fn create(
        db: &mut Db,
        slug: &str,
        name: &str,
        description: &str,
        manufacturer: &str,
        links: Json,
        metadata: Json,
        owner_team_id: i32,
    ) -> Result<Self, diesel::result::Error> {
        diesel::insert_into(schema::systems::table)
            .values((
                schema::systems::slug.eq(slug),
                schema::systems::name.eq(name),
                schema::systems::description.eq(description),
                schema::systems::manufacturer.eq(manufacturer),
                schema::systems::links.eq(links),
                schema::systems::metadata.eq(metadata),
                schema::systems::owner_team_id.eq(owner_team_id),
            ))
            .get_result(db)
            .await
    }

    pub async fn get(db: &mut Db, id: IdOrSlug<'_>) -> Result<Option<Self>, diesel::result::Error> {
        let mut query = schema::systems::table.into_boxed();
        if let Some(id) = id.as_id() {
            query = query.filter(schema::systems::dsl::id.eq(id));
        } else if let Some(slug) = id.as_slug() {
            query = query.filter(schema::systems::dsl::slug.eq(slug));
        }

        query.first::<Self>(db).await.optional()
    }
}
