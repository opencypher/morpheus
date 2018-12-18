SET SCHEMA hive_interactions.customers;

CREATE GRAPH TYPE interactions (
  	-- element types
    Policy (policyAccountNumber STRING) KEY pk (policyAccountNumber),
    Customer (customerIdx INTEGER, customerId STRING, customerName STRING) KEY ck (customerIdx),
    CustomerRep (empNo INTEGER, empName STRING) KEY ek (empNo),
    Interaction (interactionId INTEGER, date STRING, type STRING, outcomeScore STRING) KEY ik (interactionId),
    AccountHolder (accountHolderId STRING) KEY ak (accountHolderId),
    HAS_CUSTOMER_REP,
    HAS_ACCOUNT_HOLDER,
    HAS_CUSTOMER,
    HAS_POLICY,

    -- node types
    (Policy),
    (Customer),
    (Interaction),
    (CustomerRep),
    (AccountHolder),

    -- relationship types
    (Interaction)-[HAS_POLICY]->(Policy),
    (Interaction)-[HAS_CUSTOMER]->(Customer),
    (Interaction)-[HAS_CUSTOMER_REP]->(CustomerRep),
    (Interaction)-[HAS_ACCOUNT_HOLDER]->(AccountHolder)
)

CREATE GRAPH interactions_seed OF interactions (

  (Policy) FROM POLICIES_SEED,
  (Customer) FROM CUSTOMERS_SEED,
  (Interaction) FROM INTERACTIONS_SEED,
  (CustomerRep) FROM CUSTOMER_REPS_SEED,
  (AccountHolder) FROM ACCOUNT_HOLDERS_SEED,

  (Interaction)-[HAS_ACCOUNT_HOLDER]->(AccountHolder) FROM HAS_ACCOUNT_HOLDERS_SEED edge
    START NODES (Interaction) FROM INTERACTIONS_SEED start_nodes
      JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
    END NODES (AccountHolder) FROM ACCOUNT_HOLDERS_SEED end_nodes
      JOIN ON end_nodes.ACCOUNTHOLDERID = edge.ACCOUNTHOLDERID,

  (Interaction)-[HAS_POLICY]->(Policy) FROM HAS_POLICIES_SEED edge
    START NODES (Interaction) FROM INTERACTIONS_SEED start_nodes
      JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
    END NODES (Policy) FROM POLICIES_SEED end_nodes
      JOIN ON end_nodes.POLICYACCOUNTNUMBER = edge.POLICYACCOUNTNUMBER,

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

CREATE GRAPH interactions_delta OF interactions (

  (Policy) FROM POLICIES_DELTA,
  (Customer) FROM CUSTOMERS_DELTA,
  (Interaction) FROM INTERACTIONS_DELTA,
  (CustomerRep) FROM CUSTOMER_REPS_DELTA,
  (AccountHolder) FROM ACCOUNT_HOLDERS_DELTA,

  (Interaction)-[HAS_ACCOUNT_HOLDER]->(AccountHolder) FROM HAS_ACCOUNT_HOLDERS_DELTA edge
    START NODES (Interaction) FROM INTERACTIONS_DELTA start_nodes
      JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
    END NODES (AccountHolder) FROM ACCOUNT_HOLDERS_DELTA end_nodes
      JOIN ON end_nodes.ACCOUNTHOLDERID = edge.ACCOUNTHOLDERID,

  (Interaction)-[HAS_POLICY]->(Policy) FROM HAS_POLICIES_DELTA edge
    START NODES (Interaction) FROM INTERACTIONS_DELTA start_nodes
      JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
    END NODES (Policy) FROM POLICIES_DELTA end_nodes
      JOIN ON end_nodes.POLICYACCOUNTNUMBER = edge.POLICYACCOUNTNUMBER,

  (Interaction)-[HAS_CUSTOMER_REP]->(CustomerRep) FROM HAS_CUSTOMER_REPS_DELTA edge
    START NODES (Interaction) FROM INTERACTIONS_DELTA start_nodes
      JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
    END NODES (CustomerRep) FROM CUSTOMER_REPS_DELTA end_nodes
      JOIN ON end_nodes.EMPNO = edge.EMPNO,

  (Interaction)-[HAS_CUSTOMER]->(Customer) FROM HAS_CUSTOMERS_DELTA edge
    START NODES (Interaction) FROM INTERACTIONS_DELTA start_nodes
      JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
    END NODES (Customer) FROM CUSTOMERS_DELTA end_nodes
      JOIN ON end_nodes.CUSTOMERIDX = edge.CUSTOMERIDX
)
