SET SCHEMA hive.customers;

CREATE GRAPH debug (

  -- element types
  Customer (customerIdx INTEGER, customerId STRING, customerName STRING) KEY ck (customerIdx),
  CustomerRep (empNo INTEGER, empName STRING) KEY ek (empNo),
  Interaction (interactionId INTEGER, date STRING, type STRING) KEY ik (interactionId),
  HAS_CUSTOMER_REP,
  HAS_CUSTOMER,

  (Customer) FROM CUSTOMERS_SEED,
  (Interaction) FROM INTERACTIONS_SEED,
  (CustomerRep) FROM CUSTOMER_REPS_SEED,

  (Interaction)-[HAS_CUSTOMER_REP]->(CustomerRep) FROM HAS_CUSTOMER_REPS_SEED edge
    START NODES (Interaction) FROM INTERACTIONS_SEED start_nodes
      JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
    END NODES (CustomerRep) FROM CUSTOMER_REPS_SEED end_nodes
      JOIN ON end_nodes.EMPNO = edge.EMPNO,

  (Interaction)-[HAS_CUSTOMER]->(Customer) FROM HAS_CUSTOMERS_SEED edge
    START NODES (Interaction) FROM INTERACTIONS_SEED start_nodes
      JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
    END NODES (Customer) FROM CUSTOMERS_SEED end_nodes
      JOIN ON end_nodes.CUSTOMERIDX = edge.CUSTOMERIDX
)
