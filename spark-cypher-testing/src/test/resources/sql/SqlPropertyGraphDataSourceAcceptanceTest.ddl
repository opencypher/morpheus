--
-- Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
--

-- Below means <datasourceName.schemaName>
SET SCHEMA DS.SQLPGDS;

-- =========================================
-- SCHEMA
-- =========================================

-- Node labels
CREATE ELEMENT TYPE A ( name STRING ) 
CREATE ELEMENT TYPE B ( type STRING, size INTEGER? )
CREATE ELEMENT TYPE C ( name STRING ) 

-- Relationship types
CREATE ELEMENT TYPE R ( since INTEGER, before BOOLEAN? )
CREATE ELEMENT TYPE S ( since INTEGER ) 
CREATE ELEMENT TYPE T

CREATE GRAPH TYPE testSchema (
  -- Nodes
  (A), (B), (C), (A,B), (A,C),

  -- Edges
  [R], [S], [T],

  -- Constraints
  (A) - [R] -> (B),
  (B) - [R] -> (A,B),
  (A,B) - [S] -> (A,B),
  (A,C) - [T] -> (A,B)
)

-- =========================================
-- MAPPING
-- =========================================

-- GRAPH
CREATE GRAPH test OF testSchema (

  (A) FROM A,
  (B) FROM B,
  (C) FROM C,
  (A, B) FROM A_B,
  (A, C) FROM A_C,

  [R]
    FROM R edge
      START NODES (A) FROM A node
        JOIN ON node.id = edge.source
      END NODES (B) FROM B node
        JOIN ON node.id = edge.target

    FROM R edge
      START NODES (B) FROM B node
        JOIN ON node.id = edge.source
      END NODES (A, B) FROM A_B node
        JOIN ON node.id = edge.target,

  [S]
    FROM S edge
      START NODES (A, B) FROM A_B node
        JOIN ON node.id = edge.source
      END NODES (A, B) FROM A_B node
        JOIN ON node.id = edge.target,

  [T]
    FROM T edge
      START NODES (A, C) FROM A_C node
        JOIN ON node.id = edge.source
      END NODES (A, B) FROM A_B node
        JOIN ON node.id = edge.target
)