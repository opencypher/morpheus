--
-- Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
--

-- Below means <datasourceName.schemaName>
SET SCHEMA DS.SQLPGDS;

-- =========================================
-- SCHEMA
-- =========================================

-- Node labels
CATALOG CREATE LABEL (A {name: STRING})
CATALOG CREATE LABEL (B {type: STRING, size: INTEGER?})
CATALOG CREATE LABEL (C {name: STRING})

-- Relationship types
CATALOG CREATE LABEL (R {since: INTEGER, before: BOOLEAN?})
CATALOG CREATE LABEL (S {since: INTEGER})
CATALOG CREATE LABEL (T)

CREATE GRAPH SCHEMA testSchema
  -- Nodes
  (A), (B), (C), (A,B), (A,C)

  -- Edges
  [R], [S], [T]

  -- Constraints
  (A) - [R] -> (B)
  (B) - [R] -> (A,B)
  (A,B) - [S] -> (A,B)
  (A,C) - [T] -> (A,B)

-- =========================================
-- MAPPING
-- =========================================

-- GRAPH
CREATE GRAPH test WITH GRAPH SCHEMA testSchema

  NODE LABEL SETS (
    (A) FROM A,
    (B) FROM B,
    (C) FROM C,
    (A, B) FROM A_B,
    (A, C) FROM A_C
  )

  RELATIONSHIP LABEL SETS (
    (R)
      FROM R edge
        START NODES
          LABEL SET (A) FROM A node
          JOIN ON node.id = edge.source
        END NODES
          LABEL SET (B) FROM B node
          JOIN ON node.id = edge.target

      FROM R edge
        START NODES
          LABEL SET (B) FROM B node
          JOIN ON node.id = edge.source
        END NODES
          LABEL SET (A, B) FROM A_B node
          JOIN ON node.id = edge.target

    (S)
      FROM S edge
        START NODES
          LABEL SET (A, B) FROM A_B node
          JOIN ON node.id = edge.source
        END NODES
          LABEL SET (A, B) FROM A_B node
          JOIN ON node.id = edge.target

    (T)
      FROM T edge
        START NODES
          LABEL SET (A, C) FROM A_C node
          JOIN ON node.id = edge.source
        END NODES
          LABEL SET (A, B) FROM A_B node
          JOIN ON node.id = edge.target
  )
