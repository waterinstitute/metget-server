
CREATE TABLE gfs_ncep(id SERIAL PRIMARY KEY, forecastcycle TIMESTAMP NOT NULL, forecasttime TIMESTAMP NOT NULL, tau INTEGER NOT NULL, filepath VARCHAR(256) NOT NULL, url VARCHAR(256) NOT NULL, accessed TIMESTAMP NOT NULL);

CREATE TABLE gefs_fcst(id SERIAL PRIMARY KEY, forecastcycle TIMESTAMP NOT NULL, forecasttime TIMESTAMP NOT NULL, ensemble_member VARCHAR(32) NOT NULL, tau INTEGER NOT NULL, filepath VARCHAR(256) NOT NULL, url VARCHAR(256) NOT NULL, accessed TIMESTAMP NOT NULL);

CREATE TABLE hwrf(id SERIAL PRIMARY KEY, stormname VARCHAR(256) NOT NULL, forecastcycle TIMESTAMP NOT NULL, forecasttime TIMESTAMP NOT NULL, tau INTEGER NOT NULL, filepath VARCHAR(256) NOT NULL, url VARCHAR(256) NOT NULL, accessed TIMESTAMP NOT NULL);

CREATE TABLE nam_ncep(id SERIAL PRIMARY KEY, forecastcycle TIMESTAMP NOT NULL, forecasttime TIMESTAMP NOT NULL, tau INTEGER NOT NULL, filepath VARCHAR(256) NOT NULL, url VARCHAR(256) NOT NULL, accessed TIMESTAMP NOT NULL);

CREATE TABLE nhc_btk(id SERIAL PRIMARY KEY, storm_year INTEGER NOT NULL, basin VARCHAR(256) NOT NULL, storm INTEGER NOT NULL, advisory_start TIMESTAMP NOT NULL, advisory_end TIMESTAMP NOT NULL, advisory_duration_hr INT NOT NULL, filepath VARCHAR(256) NOT NULL, md5 VARCHAR(32) NOT NULL, accessed TIMESTAMP NOT NULL, geometry_data JSON NOT NULL );

CREATE TABLE nhc_fcst(id SERIAL PRIMARY KEY, storm_year INTEGER NOT NULL, basin VARCHAR(256) NOT NULL, storm INTEGER NOT NULL, advisory VARCHAR(256) NOT NULL, advisory_start TIMESTAMP NOT NULL, advisory_end TIMESTAMP NOT NULL, advisory_duration_hr INT NOT NULL, filepath VARCHAR(256) NOT NULL, md5 VARCHAR(32) NOT NULL, accessed TIMESTAMP NOT NULL, geometry_data JSON NOT NULL);

CREATE TABLE coamps_tc(id SERIAL PRIMARY KEY, stormname VARCHAR(256) NOT NULL, forecastcycle TIMESTAMP NOT NULL, forecasttime TIMESTAMP NOT NULL, tau INTEGER NOT NULL, filepath VARCHAR(512) NOT NULL, accessed TIMESTAMP NOT NULL);

CREATE TABLE hrrr_ncep(id SERIAL PRIMARY KEY, forecastcycle TIMESTAMP NOT NULL, forecasttime TIMESTAMP NOT NULL, tau INTEGER NOT NULL, filepath VARCHAR(256) NOT NULL, url VARCHAR(256) NOT NULL, accessed TIMESTAMP NOT NULL);

CREATE TABLE hrrr_alaska_ncep(id SERIAL PRIMARY KEY, forecastcycle TIMESTAMP NOT NULL, forecasttime TIMESTAMP NOT NULL, tau INTEGER NOT NULL, filepath VARCHAR(256) NOT NULL, url VARCHAR(256) NOT NULL, accessed TIMESTAMP NOT NULL);

CREATE TABLE wpc_ncep(id SERIAL PRIMARY KEY, forecastcycle TIMESTAMP NOT NULL, forecasttime TIMESTAMP NOT NULL, tau INTEGER NOT NULL, filepath VARCHAR(256) NOT NULL, url VARCHAR(256) NOT NULL, accessed TIMESTAMP NOT NULL);
