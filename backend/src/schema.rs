// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "user_team_role"))]
    pub struct UserTeamRole;
}

diesel::table! {
    artifacts (id) {
        id -> Int4,
        #[max_length = 255]
        filename -> Varchar,
        #[max_length = 255]
        mime_type -> Varchar,
        created_at -> Timestamp,
        md5 -> Bytea,
        sha256 -> Bytea,
        size -> Int4,
        #[max_length = 255]
        download_url -> Nullable<Varchar>,
        sha1 -> Bytea,
    }
}

diesel::table! {
    core_release_artifacts (core_release_id, artifact_id) {
        core_release_id -> Int4,
        artifact_id -> Int4,
    }
}

diesel::table! {
    core_releases (id) {
        id -> Int4,
        #[max_length = 255]
        version -> Varchar,
        notes -> Text,
        date_released -> Timestamp,
        prerelease -> Bool,
        yanked -> Bool,
        links -> Jsonb,
        metadata -> Jsonb,
        uploader_id -> Int4,
        core_id -> Int4,
        platform_id -> Int4,
    }
}

diesel::table! {
    core_systems (core_id, system_id) {
        core_id -> Int4,
        system_id -> Int4,
    }
}

diesel::table! {
    core_tags (tag_id, core_id) {
        core_id -> Int4,
        tag_id -> Int4,
    }
}

diesel::table! {
    cores (id) {
        id -> Int4,
        #[max_length = 255]
        slug -> Varchar,
        name -> Varchar,
        description -> Text,
        metadata -> Jsonb,
        links -> Jsonb,
        owner_team_id -> Int4,
    }
}

diesel::table! {
    files (id) {
        id -> Int4,
        data -> Bytea,
    }
}

diesel::table! {
    game_artifacts (game_id, artifact_id) {
        game_id -> Int4,
        artifact_id -> Int4,
    }
}

diesel::table! {
    games (id) {
        id -> Int4,
        #[max_length = 255]
        name -> Varchar,
        description -> Varchar,
        #[max_length = 255]
        short_description -> Varchar,
        year -> Int4,
        publisher -> Varchar,
        developer -> Varchar,
        links -> Jsonb,
        system_id -> Int4,
        system_unique_id -> Int4,
    }
}

diesel::table! {
    platform_tags (tag_id, platform_id) {
        platform_id -> Int4,
        tag_id -> Int4,
    }
}

diesel::table! {
    platforms (id) {
        id -> Int4,
        #[max_length = 255]
        slug -> Varchar,
        #[max_length = 255]
        name -> Varchar,
        description -> Text,
        links -> Jsonb,
        metadata -> Jsonb,
        owner_team_id -> Int4,
    }
}

diesel::table! {
    system_release_artifacts (artifact_id, system_release_id) {
        system_release_id -> Int4,
        artifact_id -> Int4,
    }
}

diesel::table! {
    system_releases (id) {
        id -> Int4,
        version -> Varchar,
        note -> Text,
        date_released -> Timestamp,
        prerelease -> Bool,
        yanked -> Bool,
        links -> Jsonb,
        metadata -> Jsonb,
        uploader_id -> Int4,
        system_id -> Int4,
    }
}

diesel::table! {
    system_tags (tag_id, system_id) {
        system_id -> Int4,
        tag_id -> Int4,
    }
}

diesel::table! {
    systems (id) {
        id -> Int4,
        #[max_length = 255]
        slug -> Varchar,
        name -> Varchar,
        description -> Text,
        manufacturer -> Varchar,
        links -> Jsonb,
        metadata -> Jsonb,
        owner_team_id -> Int4,
    }
}

diesel::table! {
    tags (id) {
        id -> Int4,
        #[max_length = 255]
        slug -> Varchar,
        description -> Nullable<Text>,
        color -> Int8,
    }
}

diesel::table! {
    teams (id) {
        id -> Int4,
        #[max_length = 255]
        slug -> Varchar,
        name -> Varchar,
        description -> Text,
        links -> Jsonb,
        metadata -> Jsonb,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::UserTeamRole;

    user_teams (team_id, user_id) {
        team_id -> Int4,
        user_id -> Int4,
        role -> UserTeamRole,
        invite_from -> Nullable<Int4>,
    }
}

diesel::table! {
    users (id) {
        id -> Int4,
        #[max_length = 255]
        username -> Nullable<Varchar>,
        #[max_length = 255]
        display_name -> Nullable<Varchar>,
        #[max_length = 255]
        avatar_url -> Nullable<Varchar>,
        #[max_length = 255]
        email -> Varchar,
        #[max_length = 255]
        auth_provider -> Nullable<Varchar>,
        need_reset -> Bool,
        deleted -> Bool,
        description -> Text,
        links -> Jsonb,
        metadata -> Jsonb,
    }
}

diesel::joinable!(core_release_artifacts -> artifacts (artifact_id));
diesel::joinable!(core_release_artifacts -> core_releases (core_release_id));
diesel::joinable!(core_releases -> cores (core_id));
diesel::joinable!(core_releases -> platforms (platform_id));
diesel::joinable!(core_releases -> users (uploader_id));
diesel::joinable!(core_systems -> cores (core_id));
diesel::joinable!(core_systems -> systems (system_id));
diesel::joinable!(core_tags -> cores (core_id));
diesel::joinable!(core_tags -> tags (tag_id));
diesel::joinable!(cores -> teams (owner_team_id));
diesel::joinable!(files -> artifacts (id));
diesel::joinable!(game_artifacts -> artifacts (artifact_id));
diesel::joinable!(game_artifacts -> games (game_id));
diesel::joinable!(games -> systems (system_id));
diesel::joinable!(platform_tags -> platforms (platform_id));
diesel::joinable!(platform_tags -> tags (tag_id));
diesel::joinable!(platforms -> teams (owner_team_id));
diesel::joinable!(system_release_artifacts -> artifacts (artifact_id));
diesel::joinable!(system_release_artifacts -> system_releases (system_release_id));
diesel::joinable!(system_releases -> systems (system_id));
diesel::joinable!(system_releases -> users (uploader_id));
diesel::joinable!(systems -> teams (owner_team_id));
diesel::joinable!(user_teams -> teams (team_id));

diesel::allow_tables_to_appear_in_same_query!(
    artifacts,
    core_release_artifacts,
    core_releases,
    core_systems,
    core_tags,
    cores,
    files,
    game_artifacts,
    games,
    platform_tags,
    platforms,
    system_release_artifacts,
    system_releases,
    system_tags,
    systems,
    tags,
    teams,
    user_teams,
    users,
);
