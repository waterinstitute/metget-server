-- Migration: Global RTOFS raw-file index table
-- Description: Adds the rtofs table which indexes the Global RTOFS 3-D z-level daily
--              NetCDF files (temperature and salinity) downloaded from NOMADS and
--              archived in the MetGet S3 bucket. These files are served raw via the
--              API for downstream baroclinic forcing generation (ADCIRC fort.11.nc)
--              and are not part of the build pipeline. The tau value is signed: the
--              n024 analysis step is -24.
--
-- Run this version if you have an existing database that predates the RTOFS data
-- source. It is idempotent (uses IF NOT EXISTS and a guarded ALTER TABLE) and safe
-- to run more than once.

CREATE TABLE IF NOT EXISTS rtofs(
  id SERIAL PRIMARY KEY,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  param VARCHAR(32) NOT NULL,
  filepath VARCHAR(512) NOT NULL,
  url VARCHAR(512) NOT NULL,
  accessed TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS rtofs_forecastcycle_idx ON rtofs USING brin (forecastcycle);
CREATE INDEX IF NOT EXISTS idx_rtofs_lookup ON rtofs (forecastcycle, forecasttime, param);

DO $$
BEGIN
  ALTER TABLE rtofs ADD CONSTRAINT uq_rtofs_cycle_forecast_param
    UNIQUE (forecastcycle, forecasttime, param);
EXCEPTION
  WHEN duplicate_object THEN NULL;
  WHEN duplicate_table THEN NULL;
END $$;
