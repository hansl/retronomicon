-- Your SQL goes here

CREATE TABLE core_systems
(
    core_id   integer NOT NULL REFERENCES cores,
    system_id integer NOT NULL REFERENCES systems,
    CONSTRAINT core_systems_pkey PRIMARY KEY (core_id, system_id)
);

INSERT INTO core_systems (core_id, system_id)
SELECT cores.id as core_id, cores.system_id as system_id
FROM cores;

ALTER TABLE cores
    DROP COLUMN system_id;
