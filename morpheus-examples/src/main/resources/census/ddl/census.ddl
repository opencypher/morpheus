-- format for below is: <dataSourceName>.<schemaName>
SET SCHEMA CENSUS.CENSUS;

CREATE GRAPH Census_1901 (

  -- Nodes
  LicensedDog (
    licence_number INTEGER
  ) KEY LicensedDog_NK (licence_number),

  Person (
    first_name STRING?,
    last_name STRING?
  ),

  Visitor (
    date_of_entry STRING,
    sequence INTEGER,
    nationality STRING?,
    age INTEGER?
  ) KEY Visitor_NK (date_of_entry, sequence),

  Resident (
    person_number STRING
  ) KEY Resident_NK (person_number),

  Town (
    CITY_NAME STRING,
    REGION STRING
  ) KEY Town_NK (REGION, CITY_NAME),

  -- Relationships
  PRESENT_IN,
  LICENSED_BY (
    date_of_licence STRING
  ),

  -- Node mappings:
  (Visitor, Person)
       FROM VIEW_VISITOR,

  (LicensedDog)
       FROM VIEW_LICENSED_DOG,

  (Town)
       FROM TOWN,

  (Resident, Person)
       FROM VIEW_RESIDENT,

  -- Relationship mappings:
  (Person, Resident)-[PRESENT_IN]->(Town)
      FROM VIEW_RESIDENT_ENUMERATED_IN_TOWN edge
          START NODES (Person, Resident)
              FROM VIEW_RESIDENT start_nodes
                  JOIN ON start_nodes.PERSON_NUMBER = edge.PERSON_NUMBER
          END NODES (Town)
              FROM TOWN end_nodes
                  JOIN ON end_nodes.REGION = edge.REGION
                  AND end_nodes.CITY_NAME = edge.CITY_NAME,
  (Person, Visitor)-[PRESENT_IN]->(Town)
      FROM VIEW_VISITOR_ENUMERATED_IN_TOWN edge
          START NODES (Person, Visitor)
              FROM VIEW_VISITOR start_nodes
                  JOIN ON start_nodes.NATIONALITY = edge.COUNTRYOFORIGIN
                  AND start_nodes.PASSPORT_NUMBER = edge.PASSPORT_NO
          END NODES (Town)
              FROM TOWN end_nodes
                  JOIN ON end_nodes.REGION = edge.REGION
                  AND end_nodes.CITY_NAME = edge.CITY_NAME,
  (LicensedDog)-[PRESENT_IN]->(Town)
      FROM VIEW_LICENSED_DOG edge
          START NODES (LicensedDog)
              FROM VIEW_LICENSED_DOG start_nodes
                  JOIN ON start_nodes.LICENCE_NUMBER = edge.LICENCE_NUMBER
          END NODES (Town)
              FROM TOWN end_nodes
                  JOIN ON end_nodes.REGION = edge.REGION
                  AND end_nodes.CITY_NAME = edge.CITY_NAME,

  (LicensedDog)-[LICENSED_BY]->(Person, Resident)
      FROM VIEW_LICENSED_DOG edge
          START NODES (LicensedDog)
              FROM VIEW_LICENSED_DOG start_nodes
                  JOIN ON start_nodes.LICENCE_NUMBER = edge.LICENCE_NUMBER
          END NODES (Person, Resident)
              FROM VIEW_RESIDENT end_nodes
                  JOIN ON end_nodes.PERSON_NUMBER = edge.PERSON_NUMBER
)
