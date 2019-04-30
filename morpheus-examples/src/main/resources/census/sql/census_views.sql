CREATE VIEW CENSUS.view_person AS
SELECT
  first_name,
  last_name
 FROM
  CENSUS.residents;
  
CREATE VIEW CENSUS.view_visitor AS
SELECT
  first_name,
  last_name,
  iso3166 as nationality,
  passport_number,
  date_of_entry,
  entry_sequence as sequence,
  age
FROM
  CENSUS.visitors;

CREATE VIEW CENSUS.view_resident AS
SELECT
  first_name,
  last_name,
  person_number
 FROM
  CENSUS.residents;

CREATE VIEW CENSUS.view_licensed_dog AS
SELECT
  person_number,
  licence_number,
  licence_date as date_of_licence,
  region,
  city_name
FROM
  CENSUS.licensed_dogs;

CREATE VIEW CENSUS.view_resident_enumerated_in_town AS
SELECT
  PERSON_NUMBER,
  REGION,
  CITY_NAME
FROM
  CENSUS.residents;

CREATE VIEW CENSUS.view_visitor_enumerated_in_town AS
SELECT
  ISO3166 AS countryOfOrigin,
  PASSPORT_NUMBER AS PASSPORT_NO,
  REGION,
  CITY_NAME
FROM
  CENSUS.visitors;