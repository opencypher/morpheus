-- tag::full-example[]
SET SCHEMA myH2Source.myH2Schema

CREATE GRAPH myGraph (
  Person (name STRING),
  KNOWS (since INTEGER),

  (Person) FROM view_Persons,

  (Person)-[KNOWS]->(Person)
    FROM view_KNOWS knows
      START NODES (Person) FROM view_Persons person
        JOIN ON person.id = knows.start_id
      END NODES (Person) FROM view_Persons person
        JOIN ON person.id = knows.end_id
)
-- end::full-example[]
