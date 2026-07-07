-- Migration: JTWC Best Track / Forecast tables
-- Description: Adds the jtwc_btk and jtwc_fcst tables that hold JTWC (Western Pacific,
--              North Indian Ocean, and Southern Hemisphere) storm data. The data is stored
--              in the same ATCF best-track / forecast format as the NHC data so that
--              downstream consumers require no new input format support.
--
-- Run this version if you have an existing database that predates the JTWC meteorological
-- source. It is idempotent (uses IF NOT EXISTS) and safe to run more than once.

CREATE TABLE IF NOT EXISTS jtwc_btk(
  id SERIAL PRIMARY KEY,
  storm_year INTEGER NOT NULL,
  basin VARCHAR(256) NOT NULL,
  storm INTEGER NOT NULL,
  advisory_start TIMESTAMP NOT NULL,
  advisory_end TIMESTAMP NOT NULL,
  advisory_duration_hr INT NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  md5 VARCHAR(32) NOT NULL,
  accessed TIMESTAMP NOT NULL,
  geometry_data JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS jtwc_fcst(
  id SERIAL PRIMARY KEY,
  storm_year INTEGER NOT NULL,
  basin VARCHAR(256) NOT NULL,
  storm INTEGER NOT NULL,
  advisory VARCHAR(256) NOT NULL,
  advisory_start TIMESTAMP NOT NULL,
  advisory_end TIMESTAMP NOT NULL,
  advisory_duration_hr INT NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  md5 VARCHAR(32) NOT NULL,
  accessed TIMESTAMP NOT NULL,
  geometry_data JSON NOT NULL
);

CREATE INDEX IF NOT EXISTS jtwc_fcst_basin_idx ON jtwc_fcst USING brin (basin);
CREATE INDEX IF NOT EXISTS jtwc_btk_basin_idx ON jtwc_btk USING brin (basin);
