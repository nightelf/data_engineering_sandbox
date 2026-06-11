-- Run against the data_engineering database.
-- Per-state rollup of the zip_codes data (primary records only).

CREATE TABLE IF NOT EXISTS state_zip_codes_analysis (
    id                  SERIAL PRIMARY KEY,
    state               TEXT,
    state_full_name     TEXT,
    number_zip_codes    INTEGER,
    population          INTEGER,
    population_white    INTEGER,
    population_black    INTEGER,
    population_hispanic INTEGER,
    population_asian    INTEGER,
    population_male     INTEGER,
    population_female   INTEGER,
    highest_elevation   INTEGER
);
