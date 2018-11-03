SET SCHEMA hive.customers;

CREATE GRAPH interactions WITH GRAPH SCHEMA (
	-- node labels
    LABEL ( Policy
        { policyAccountNumber  : STRING}
      ),
    LABEL ( Customer
        { customerIdx          : INTEGER,
          customerId           : STRING,
          customerName         : STRING}
      ),
    LABEL ( CustomerRep
        { empNo                : INTEGER,
          empName              : STRING}
      ),
    LABEL ( Interaction
        { interactionId        : INTEGER,
          date                 : STRING,
          type                 : STRING,
          outcomeScore         : STRING}
      ),
    LABEL ( AccountHolder
        { accountHolderId      : STRING}
      ),
      -- relationship labels
    LABEL ( HAS_CUSTOMER_REP ),
    LABEL ( HAS_ACCOUNT_HOLDER ),
    LABEL ( HAS_CUSTOMER ),
    LABEL ( HAS_POLICY ),

    	-- node label sets
    (Policy),
    (Customer),
    (Interaction),
    (CustomerRep),
    (AccountHolder),

    	-- relationship label sets
    [HAS_CUSTOMER],
    [HAS_ACCOUNT_HOLDER],
    [HAS_CUSTOMER_REP],
    [HAS_POLICY],

    	-- relationship triplets
    (Interaction)     <0 .. *>    - [HAS_POLICY] ->        <1>        (Policy),
    (Interaction)     <0 .. *>    - [HAS_CUSTOMER] ->      <1>        (Customer),
    (Interaction)     <0 .. *>    - [HAS_CUSTOMER_REP] ->  <1>        (CustomerRep),
    (Interaction)     <0 .. *>    - [HAS_ACCOUNT_HOLDER] ->   <1>        (AccountHolder)
    )

    NODE LABEL SETS (
        (Policy)
             FROM POLICIES,

        (Customer)
             FROM CUSTOMERS,

        (Interaction)
             FROM INTERACTIONS,

        (CustomerRep)
             FROM CUSTOMER_REPS,

        (AccountHolder)
             FROM ACCOUNT_HOLDERS
    )

    RELATIONSHIP LABEL SETS (

        (HAS_ACCOUNT_HOLDER)
            FROM HAS_ACCOUNT_HOLDERS edge
                START NODES
                    LABEL SET (Interaction)
                    FROM INTERACTIONS start_nodes
                        JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
                END NODES
                    LABEL SET (AccountHolder)
                    FROM ACCOUNT_HOLDERS end_nodes
                        JOIN ON end_nodes.ACCOUNTHOLDERID = edge.ACCOUNTHOLDERID,

        (HAS_POLICY)
            FROM HAS_POLICIES edge
                START NODES
                    LABEL SET (Interaction)
                    FROM INTERACTIONS start_nodes
                        JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
                END NODES
                    LABEL SET (Policy)
                    FROM POLICIES end_nodes
                        JOIN ON end_nodes.POLICYACCOUNTNUMBER = edge.POLICYACCOUNTNUMBER,

        (HAS_CUSTOMER_REP)
            FROM HAS_CUSTOMER_REPS edge
                START NODES
                    LABEL SET (Interaction)
                    FROM INTERACTIONS start_nodes
                        JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
                END NODES
                    LABEL SET (CustomerRep)
                    FROM CUSTOMER_REPS end_nodes
                        JOIN ON end_nodes.EMPNO = edge.EMPNO,

        (HAS_CUSTOMER)
            FROM HAS_CUSTOMERS edge
                START NODES
                    LABEL SET (Interaction)
                    FROM INTERACTIONS start_nodes
                        JOIN ON start_nodes.INTERACTIONID = edge.INTERACTIONID
                END NODES
                    LABEL SET (Customer)
                    FROM CUSTOMERS end_nodes
                        JOIN ON end_nodes.CUSTOMERID = edge.CUSTOMERID
    )
