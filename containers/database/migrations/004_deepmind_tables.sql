-- Migration: Google DeepMind cyclone ensemble forecast table
-- Description: Adds the deepmind_fcst table that holds Google DeepMind Weather Lab
--              cyclone ensemble forecast data (ATCF format, 50 members + ensemble
--              mean), partitioned by (forecastcycle, storm_year, basin, storm,
--              ensemble_member). The data is stored in the same ATCF forecast format
--              as the NHC/JTWC forecast data so that downstream consumers require no
--              new input format support. `ensemble_member` holds the ATCF tech id
--              (e.g. "F007") or the literal string "mean" for the ensemble-mean
--              product (raw tech "FNV3").
--
-- Run this version if you have an existing database that predates the DeepMind
-- meteorological source. It is idempotent (uses IF NOT EXISTS and a guarded ALTER
-- TABLE) and safe to run more than once.

CREATE TABLE IF NOT EXISTS deepmind_fcst(
  id SERIAL PRIMARY KEY,
  forecastcycle TIMESTAMP NOT NULL,
  storm_year INTEGER NOT NULL,
  basin VARCHAR(256) NOT NULL,
  storm VARCHAR(256) NOT NULL,
  ensemble_member VARCHAR(32) NOT NULL,
  advisory_start TIMESTAMP NOT NULL,
  advisory_end TIMESTAMP NOT NULL,
  advisory_duration_hr INT NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  md5 VARCHAR(32) NOT NULL,
  accessed TIMESTAMP NOT NULL,
  geometry_data JSON NOT NULL
);

CREATE INDEX IF NOT EXISTS deepmind_fcst_basin_idx ON deepmind_fcst USING brin (basin);
CREATE INDEX IF NOT EXISTS deepmind_fcst_forecastcycle_idx ON deepmind_fcst USING brin (forecastcycle);
CREATE INDEX IF NOT EXISTS idx_deepmind_lookup ON deepmind_fcst (forecastcycle, storm_year, basin, storm, ensemble_member);

DO $$
BEGIN
  ALTER TABLE deepmind_fcst ADD CONSTRAINT uq_deepmind_cycle_storm_member
    UNIQUE (forecastcycle, basin, storm, storm_year, ensemble_member);
EXCEPTION
  WHEN duplicate_object THEN NULL;
  WHEN duplicate_table THEN NULL;
END $$;
