-- format for below is: <dataSourceName>.<schemaName>
SET SCHEMA CENSUS.CENSUS;

-- =================================================================

CREATE ELEMENT TYPE LicensedDog (
  licence_number INTEGER
) KEY LicensedDog_NK (licence_number)

CREATE ELEMENT TYPE Person (first_name STRING?, last_name STRING?)

CREATE ELEMENT TYPE Visitor (
  date_of_entry STRING,
  sequence INTEGER,
  nationality STRING?,
  age INTEGER?
) KEY Visitor_NK (date_of_entry, sequence)

CREATE ELEMENT TYPE Resident (
  person_number STRING
) KEY Resident_NK (person_number)

CREATE ELEMENT TYPE Town (
  CITY_NAME STRING,
  REGION STRING
) KEY Town_NK (REGION, CITY_NAME)

CREATE ELEMENT TYPE PRESENT_IN

CREATE ELEMENT TYPE LICENSED_BY (date_of_licence STRING)

-- =================================================================

CREATE GRAPH TYPE Census (

  --NODES
  (Person, Visitor),  -- keyed by node key Visitor_NK
  (Person, Resident), -- keyed by node key Resident_NK
  (Town),
  (LicensedDog),

  --EDGES
  [PRESENT_IN],
  [LICENSED_BY],

   --EDGE CONSTRAINTS
  (Person | LicensedDog) <0 .. *> - [PRESENT_IN] -> <1>(Town),
  (LicensedDog)- [LICENSED_BY] ->(Resident)
)
-- =================================================================

CREATE GRAPH Census_1901 OF Census (
  (Visitor, Person)
       FROM VIEW_VISITOR,

  (LicensedDog)
       FROM VIEW_LICENSED_DOG,

  (Town)
       FROM TOWN,

  (Resident, Person)
       FROM VIEW_RESIDENT,

  [PRESENT_IN]
      FROM VIEW_RESIDENT_ENUMERATED_IN_TOWN edge
          START NODES (Resident, Person)
              FROM VIEW_RESIDENT start_nodes
                  JOIN ON start_nodes.PERSON_NUMBER = edge.PERSON_NUMBER
          END NODES (Town)
              FROM TOWN end_nodes
                  JOIN ON end_nodes.REGION = edge.REGION
                  AND end_nodes.CITY_NAME = edge.CITY_NAME,
      FROM VIEW_VISITOR_ENUMERATED_IN_TOWN edge
          START NODES (Visitor, Person)
              FROM VIEW_VISITOR start_nodes
                  JOIN ON start_nodes.NATIONALITY = edge.COUNTRYOFORIGIN
                  AND start_nodes.PASSPORT_NUMBER = edge.PASSPORT_NO
          END NODES (Town)
              FROM TOWN end_nodes
                  JOIN ON end_nodes.REGION = edge.REGION
                  AND end_nodes.CITY_NAME = edge.CITY_NAME,
      FROM VIEW_LICENSED_DOG edge
          START NODES (LicensedDog)
              FROM VIEW_LICENSED_DOG start_nodes
                  JOIN ON start_nodes.LICENCE_NUMBER = edge.LICENCE_NUMBER
          END NODES (Town)
              FROM TOWN end_nodes
                  JOIN ON end_nodes.REGION = edge.REGION
                  AND end_nodes.CITY_NAME = edge.CITY_NAME,

  [LICENSED_BY]
      FROM VIEW_LICENSED_DOG edge
          START NODES (LicensedDog)
              FROM VIEW_LICENSED_DOG start_nodes
                  JOIN ON start_nodes.LICENCE_NUMBER = edge.LICENCE_NUMBER
          END NODES (Resident, Person)
              FROM VIEW_RESIDENT end_nodes
                  JOIN ON end_nodes.PERSON_NUMBER = edge.PERSON_NUMBER
)
