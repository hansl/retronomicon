-- This file should undo anything in `up.sql`
ALTER TABLE cores ADD COLUMN system_id integer NULL REFERENCES systems;

UPDATE cores SET system_id = core_systems.system_id FROM core_systems WHERE cores.id = core_systems.core_id;

ALTER TABLE cores ALTER COLUMN system_id SET NOT NULL;

DROP TABLE core_systems;
