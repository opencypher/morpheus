SET SCHEMA hive.yelp

CREATE GRAPH TYPE yelp (
 Business (businessId STRING, name STRING, address STRING, city STRING),
 User (name STRING),
 REVIEWS (stars FLOAT),

 (Business),
 (User),
 (User)-[REVIEWS]->(Business)
)

CREATE GRAPH pre2017 OF yelp (
 (Business) FROM business (business_id AS businessId),
 (User) FROM user,
 (User)-[REVIEWS]->(Business) FROM pre2017 edge
   START NODES (User) FROM user node JOIN ON node.id = edge.source
   END NODES (Business) FROM business node JOIN ON node.id = edge.target
)

CREATE GRAPH since2017 OF yelp (
 (Business) FROM business (business_id AS businessId, name AS name, address AS address, city AS city),
 (User) FROM user,
 (User)-[REVIEWS]->(Business) FROM since2017 edge
   START NODES (User) FROM user node JOIN ON node.id = edge.source
   END NODES (Business) FROM business node JOIN ON node.id = edge.target
)