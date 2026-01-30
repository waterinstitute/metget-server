--
--Creates tables for the storage of metget's metadata
--
CREATE TABLE gfs_ncep(
  id SERIAL PRIMARY KEY,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  url VARCHAR(256) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
CREATE TABLE gefs_fcst(
  id SERIAL PRIMARY KEY,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  ensemble_member VARCHAR(32) NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  url VARCHAR(256) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
CREATE TABLE hwrf(
  id SERIAL PRIMARY KEY,
  stormname VARCHAR(256) NOT NULL,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  url VARCHAR(256) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
 CREATE TABLE ncep_hafs_a(
  id SERIAL PRIMARY KEY,
  stormname VARCHAR(256) NOT NULL,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  url VARCHAR(256) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
 CREATE TABLE ncep_hafs_b(
  id SERIAL PRIMARY KEY,
  stormname VARCHAR(256) NOT NULL,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  url VARCHAR(256) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
CREATE TABLE nam_ncep(
  id SERIAL PRIMARY KEY,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  url VARCHAR(256) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
CREATE TABLE nhc_btk(
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
CREATE TABLE nhc_fcst(
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
CREATE TABLE coamps_tc(
  id SERIAL PRIMARY KEY,
  stormname VARCHAR(256) NOT NULL,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(512) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
CREATE TABLE ctcx(
  id SERIAL PRIMARY KEY,
  stormname VARCHAR(256) NOT NULL,
  ensemble_member INTEGER NOT NULL,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(512) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
CREATE TABLE hrrr_ncep(
  id SERIAL PRIMARY KEY,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  url VARCHAR(256) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
CREATE TABLE hrrr_alaska_ncep(
  id SERIAL PRIMARY KEY,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  url VARCHAR(256) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
CREATE TABLE wpc_ncep(
  id SERIAL PRIMARY KEY,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  url VARCHAR(256) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
CREATE TABLE nhc_adeck(
  id SERIAL PRIMARY KEY,
  model CHAR(4) NOT NULL,
  storm_year INTEGER NOT NULL,
  basin CHAR(2) NOT NULL,
  storm INTEGER NOT NULL,
  forecastcycle TIMESTAMP NOT NULL,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP NOT NULL,
  duration INTEGER NOT NULL,
  geometry_Data JSON NOT NULL
);
CREATE TABLE rrfs_ncep(
  id SERIAL PRIMARY KEY,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  url VARCHAR(256) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
CREATE TABLE refs_ncep(
  id SERIAL PRIMARY KEY,
  forecastcycle TIMESTAMP NOT NULL,
  forecasttime TIMESTAMP NOT NULL,
  ensemble_member VARCHAR(32) NOT NULL,
  tau INTEGER NOT NULL,
  filepath VARCHAR(256) NOT NULL,
  url VARCHAR(256) NOT NULL,
  accessed TIMESTAMP NOT NULL
);
--
--Creates tables for the storage of metget's API access
--
CREATE TABLE apikeys(
  id SERIAL PRIMARY KEY,
  key CHAR(41) NOT NULL,
  username VARCHAR(256) NOT NULL,
  description VARCHAR(256),
  credit_limit BIGINT NOT NULL,
  enabled BOOLEAN NOT NULL,
  expiration TIMESTAMP,
  permissions JSON
);
--
--Creates tables for the storage of metget's build requests and status
--
CREATE TYPE metget_status AS ENUM(
  'queued', 'running', 'error', 'completed'
);
CREATE TABLE requests(
  id SERIAL PRIMARY KEY,
  request_id VARCHAR(36) NOT NULL,
  try INTEGER DEFAULT 0 NOT NULL,
  status metget_status NOT NULL,
  start_date TIMESTAMP NOT NULL,
  last_date TIMESTAMP NOT NULL,
  api_key VARCHAR(128) NOT NULL,
  credit_usage BIGINT NOT NULL,
  source_ip VARCHAR(128) NOT NULL,
  input_data JSON NOT NULL,
  message JSON NOT NULL
);
--
--Create Brin Indexes on forecastcycle for the various tables
--
CREATE INDEX gfs_ncep_forecastcycle_idx ON gfs_ncep USING brin (forecastcycle);
CREATE INDEX gefs_fcst_forecastcycle_idx ON gefs_fcst USING brin (forecastcycle);
--
-- Performance optimization: composite indexes and unique constraints for batch operations
--
-- GFS
CREATE INDEX idx_gfs_lookup ON gfs_ncep (forecastcycle, forecasttime);
ALTER TABLE gfs_ncep ADD CONSTRAINT uq_gfs_cycle_forecast
  UNIQUE (forecastcycle, forecasttime);
-- NAM
CREATE INDEX idx_nam_lookup ON nam_ncep (forecastcycle, forecasttime);
ALTER TABLE nam_ncep ADD CONSTRAINT uq_nam_cycle_forecast
  UNIQUE (forecastcycle, forecasttime);
-- HRRR
CREATE INDEX idx_hrrr_lookup ON hrrr_ncep (forecastcycle, forecasttime);
ALTER TABLE hrrr_ncep ADD CONSTRAINT uq_hrrr_cycle_forecast
  UNIQUE (forecastcycle, forecasttime);
-- HRRR Alaska
CREATE INDEX idx_hrrr_alaska_lookup ON hrrr_alaska_ncep (forecastcycle, forecasttime);
ALTER TABLE hrrr_alaska_ncep ADD CONSTRAINT uq_hrrr_alaska_cycle_forecast
  UNIQUE (forecastcycle, forecasttime);
-- RRFS
CREATE INDEX idx_rrfs_lookup ON rrfs_ncep (forecastcycle, forecasttime);
ALTER TABLE rrfs_ncep ADD CONSTRAINT uq_rrfs_cycle_forecast
  UNIQUE (forecastcycle, forecasttime);
-- WPC
CREATE INDEX idx_wpc_lookup ON wpc_ncep (forecastcycle, forecasttime);
ALTER TABLE wpc_ncep ADD CONSTRAINT uq_wpc_cycle_forecast
  UNIQUE (forecastcycle, forecasttime);
-- GEFS (ensemble)
CREATE INDEX idx_gefs_lookup ON gefs_fcst (forecastcycle, forecasttime, ensemble_member);
ALTER TABLE gefs_fcst ADD CONSTRAINT uq_gefs_cycle_forecast_member
  UNIQUE (forecastcycle, forecasttime, ensemble_member);
CREATE INDEX hwrf_forecastcycle_idx ON hwrf USING brin (forecastcycle);
CREATE INDEX hafsa_forecastcycle_idx ON ncep_hafs_a USING brin (forecastcycle);
CREATE INDEX hafsb_forecastcycle_idx ON ncep_hafs_b USING brin (forecastcycle);
CREATE INDEX nam_ncep_forecastcycle_idx ON nam_ncep USING brin (forecastcycle);
CREATE INDEX coamps_tc_forecastcycle_idx ON coamps_tc USING brin (forecastcycle);
CREATE INDEX ctcx_forecastcycle_idx ON ctcx USING brin (forecastcycle);
CREATE INDEX hrrr_ncep_forecastcycle_idx ON hrrr_ncep USING brin (forecastcycle);
CREATE INDEX hrrr_alaska_ncep_forecastcycle_idx ON hrrr_alaska_ncep USING brin (forecastcycle);
CREATE INDEX rrfs_ncep_forecastcycle_idx ON rrfs_ncep USING brin (forecastcycle);
CREATE INDEX refs_ncep_forecastcycle_idx ON refs_ncep USING brin (forecastcycle);
CREATE INDEX wpc_ncep_forecastcycle_idx ON wpc_ncep USING brin (forecastcycle);
CREATE INDEX nhc_adeck_model_idx ON nhc_adeck USING brin (model);
CREATE INDEX nhc_adeck_forecastcycle_idx ON nhc_adeck USING brin (forecastcycle);
--
-- Create Brin Index on basin for nhc_fcst and nhc_btk
--
CREATE INDEX nhc_fcst_basin_idx ON nhc_fcst USING brin (basin);
CREATE INDEX nhc_btk_basin_idx ON nhc_btk USING brin (basin);
