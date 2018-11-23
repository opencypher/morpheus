-- format for below is: <dataSourceName>.<schemaName>
SET SCHEMA H2.NORTHWIND;

-- Node labels

CATALOG CREATE LABEL Employee ({
  employeeID: INTEGER,
  lastName: STRING,
  firstName: STRING,
  title: STRING?,
  titleOfCourtesy: STRING?,
  birthDate: STRING?,
  hireDate: STRING?,
  address: STRING?,
  city: STRING?,
  region: STRING?,
  postalCode: STRING?,
  country: STRING?,
  homePhone: STRING?,
  extension: STRING?,
  reportsTo: INTEGER?,
  photoPath: STRING?
})

CATALOG CREATE LABEL Territory ({
  territoryID: STRING,
  territoryDescription: STRING,
  regionID: INTEGER
})

CATALOG CREATE LABEL Supplier ({
  supplierID: INTEGER,
  companyName: STRING,
  contactName: STRING?,
  contactTitle: STRING?,
  address: STRING?,
  city: STRING?,
  region: STRING?,
  postalCode: STRING?,
  country: STRING?,
  phone: STRING?,
  fax: STRING?,
  homePage: STRING?
})

CATALOG CREATE LABEL Customer ({
  customerID: STRING,
  companyName: STRING,
  contactName: STRING?,
  contactTitle: STRING?,
  address: STRING?,
  city: STRING?,
  region: STRING?,
  postalCode: STRING?,
  country: STRING?,
  phone: STRING?,
  fax: STRING?
})

CATALOG CREATE LABEL Product ({
  productID: INTEGER,
  productName: STRING,
  supplierID: INTEGER?,
  categoryID: INTEGER?,
  quantityPerUnit: STRING?,
  unitPrice: INTEGER?,
  unitsInStock: INTEGER?,
  unitsOnOrder: INTEGER?,
  reorderLevel: INTEGER?,
  discontinued: INTEGER
})

CATALOG CREATE LABEL OrderDetails ({
  orderID: INTEGER,
  productID: INTEGER,
  unitPrice: INTEGER,
  quantity: INTEGER,
  discount: INTEGER
})

CATALOG CREATE LABEL Category ({
  categoryID: INTEGER,
  categoryName: STRING,
  description: STRING?
})

CATALOG CREATE LABEL Region ({
  regionID: INTEGER,
  regionDescription: STRING
})

CATALOG CREATE LABEL Order ({
  orderID: INTEGER,
  customerID: STRING?,
  employeeID: INTEGER?,
  orderDate: STRING?,
  requiredDate: STRING?,
  shippedDate: STRING?,
  shipVia: INTEGER?,
  freight: INTEGER?,
  shipName: STRING?,
  shipAddress: STRING?,
  shipCity: STRING?,
  shipRegion: STRING?,
  shipPostalCode: STRING?,
  shipCountry: STRING?
})

CATALOG CREATE LABEL Shipper ({
  shipperID: INTEGER,
  companyName: STRING,
  phone: STRING?
})

CATALOG CREATE LABEL CustomerDemographic ({
  customerTypeID: STRING,
  customerDesc: STRING?
})

-- Relationship types

CATALOG CREATE LABEL HAS_SUPPLIER
CATALOG CREATE LABEL HAS_PRODUCT
CATALOG CREATE LABEL HAS_CATEGORY
CATALOG CREATE LABEL HAS_TERRITORY
CATALOG CREATE LABEL HAS_EMPLOYEE
CATALOG CREATE LABEL REPORTS_TO
CATALOG CREATE LABEL HAS_CUSTOMER
CATALOG CREATE LABEL HAS_CUSTOMER_DEMOGRAPHIC
CATALOG CREATE LABEL HAS_ORDER
CATALOG CREATE LABEL HAS_SHIPPER
CATALOG CREATE LABEL HAS_REGION

-- =================================================================

CREATE GRAPH SCHEMA NORTHWIND_NAIVE (

    -- Nodes
    (Employee),
    (Territory),
    (Supplier),
    (Customer),
    (Product),
    (OrderDetails),
    (Category),
    (Region),
    (Employee),
    (Order),
    (Shipper),
    (CustomerDemographic),

    -- Relationships
    [HAS_SUPPLIER],
    [HAS_PRODUCT],
    [HAS_CATEGORY],
    [HAS_TERRITORY],
    [HAS_EMPLOYEE],
    [REPORTS_TO],
    [HAS_CUSTOMER],
    [HAS_CUSTOMER_DEMOGRAPHIC],
    [HAS_ORDER],
    [HAS_SHIPPER],
    [HAS_REGION],

    -- Relationship type constraints
    (Product)-[HAS_SUPPLIER]->(Supplier),
    (Product)-[HAS_CATEGORY]->(Category),
    (OrderDetails)-[HAS_PRODUCT]->(Product),
    (OrderDetails)-[HAS_ORDER]->(Order),
    (Order)-[HAS_CUSTOMER]->(Customer),
    (Order)-[HAS_EMPLOYEE]->(Employee),
    (Order)-[HAS_SHIPPER]->(Shipper),
    (Employee)-[REPORTS_TO]->(Employee),
    (Territory)-[HAS_REGION]->(Region),
    -- Link tables become two relationships in either direction
    (Employee)-[HAS_TERRITORY]->(Territory),
    (Territory)-[HAS_EMPLOYEE]->(Employee),
    (Customer)-[HAS_CUSTOMER_DEMOGRAPHIC]->(CustomerDemographic),
    (CustomerDemographic)-[HAS_CUSTOMER]->(Customer)
)

-- =================================================================
CREATE GRAPH Northwind WITH GRAPH SCHEMA NORTHWIND_NAIVE (
    NODE LABEL SETS (
        (Order)
             FROM VIEW_ORDERS,

        (Territory)
             FROM VIEW_TERRITORIES,

        (Employee)
             FROM VIEW_EMPLOYEES,

        (Category)
             FROM VIEW_CATEGORIES,

        (Customer)
             FROM VIEW_CUSTOMERS,

        (OrderDetails)
             FROM VIEW_ORDER_DETAILS,

        (Shipper)
             FROM VIEW_SHIPPERS,

        (Product)
             FROM VIEW_PRODUCTS,

        (Region)
             FROM VIEW_REGION,

        (CustomerDemographic)
             FROM VIEW_CUSTOMERDEMOGRAPHICS,

        (Supplier)
             FROM VIEW_SUPPLIERS
    )

    RELATIONSHIP LABEL SETS (

    		-- (Product)-[HAS_CATEGORY]->(Category)
        (HAS_CATEGORY)
            FROM VIEW_PRODUCTS edge
                START NODES
                    LABEL SET (Product)
                    -- edge table is also start table, each row joining to itself
                    FROM VIEW_PRODUCTS start_nodes
                        JOIN ON start_nodes.PRODUCTID = edge.PRODUCTID
                END NODES
                    LABEL SET (Category)
                    FROM VIEW_CATEGORIES end_nodes
                        JOIN ON end_nodes.CATEGORYID = edge.CATEGORYID,

    		-- (Territory)-[HAS_REGION]->(Region)
        (HAS_REGION)
            FROM VIEW_TERRITORIES edge
                START NODES
                    LABEL SET (Territory)
                    -- edge table is also start table, each row joining to itself
                    FROM VIEW_TERRITORIES start_nodes
                        JOIN ON start_nodes.TERRITORYID = edge.TERRITORYID
                END NODES
                    LABEL SET (Region)
                    FROM VIEW_REGION end_nodes
                        JOIN ON end_nodes.REGIONID = edge.REGIONID,

    		-- (:Employee)-[:HAS_TERRITORY]->(:Territory)
        (HAS_TERRITORY)
            FROM VIEW_EMPLOYEETERRITORIES edge
                START NODES
                    LABEL SET (Employee)
                    FROM VIEW_EMPLOYEES start_nodes
                        JOIN ON start_nodes.EMPLOYEEID = edge.EMPLOYEEID
                END NODES
                    LABEL SET (Territory)
                    FROM VIEW_TERRITORIES end_nodes
                        JOIN ON end_nodes.TERRITORYID = edge.TERRITORYID,

        (HAS_EMPLOYEE)
  			 		-- (:Territory)-[:HAS_EMPLOYEE]->(:Employee)
             FROM VIEW_EMPLOYEETERRITORIES edge
                START NODES
                    LABEL SET (Territory)
                    FROM VIEW_TERRITORIES start_nodes
                        JOIN ON start_nodes.TERRITORYID = edge.TERRITORYID
                END NODES
                    LABEL SET (Employee)
                    FROM VIEW_EMPLOYEES end_nodes
                        JOIN ON end_nodes.EMPLOYEEID = edge.EMPLOYEEID,
    				-- (:Order)-[:HAS_EMPLOYEE]->(:Employee)
            FROM VIEW_ORDERS edge
                START NODES
                    LABEL SET (Order)
                    -- edge table is also start table, each row joining to itself
                    FROM VIEW_ORDERS start_nodes
                        JOIN ON start_nodes.ORDERID = edge.ORDERID
                END NODES
                    LABEL SET (Employee)
                    FROM VIEW_EMPLOYEES end_nodes
                        JOIN ON end_nodes.EMPLOYEEID = edge.EMPLOYEEID,

    		-- (Customer)-[HAS_CUSTOMER_DEMOGRAPHIC]->(CustomerDemographic)
        (HAS_CUSTOMER_DEMOGRAPHIC)
            FROM CUSTOMERCUSTOMERDEMO edge
                START NODES
                    LABEL SET (Customer)
                    FROM VIEW_CUSTOMERS start_nodes
                        JOIN ON start_nodes.CUSTOMERID = edge.CUSTOMERID
                END NODES
                    LABEL SET (CustomerDemographic)
                    FROM VIEW_CUSTOMERDEMOGRAPHICS end_nodes
                        JOIN ON end_nodes.CUSTOMERTYPEID = edge.CUSTOMERTYPEID,

        (HAS_CUSTOMER)
   					-- (Order)-[HAS_CUSTOMER]->(Customer)
            FROM VIEW_ORDERS edge
                START NODES
                    LABEL SET (Order)
                    -- edge table is also start table, each row joining to itself
                    FROM VIEW_ORDERS start_nodes
                        JOIN ON start_nodes.ORDERID = edge.ORDERID
                END NODES
                    LABEL SET (Customer)
                    FROM VIEW_CUSTOMERS end_nodes
                        JOIN ON end_nodes.CUSTOMERID = edge.CUSTOMERID,
				    -- (CustomerDemographic)-[HAS_CUSTOMER]->(Customer)
            FROM CUSTOMERCUSTOMERDEMO edge
                START NODES
                    LABEL SET (CustomerDemographic)
                    FROM VIEW_CUSTOMERDEMOGRAPHICS start_nodes
                        JOIN ON start_nodes.CUSTOMERTYPEID = edge.CUSTOMERTYPEID
                END NODES
                    LABEL SET (Customer)
                    FROM VIEW_CUSTOMERS end_nodes
                        JOIN ON end_nodes.CUSTOMERID = edge.CUSTOMERID,

    		-- (OrderDetails)-[HAS_ORDER]->(Order)
        (HAS_ORDER)
            FROM VIEW_ORDER_DETAILS edge
                START NODES
                    LABEL SET (OrderDetails)
                    -- edge table is also start table, each row joining to itself
                    FROM VIEW_ORDER_DETAILS start_nodes
                        JOIN ON start_nodes.ORDERID = edge.ORDERID                        
                        AND start_nodes.PRODUCTID = edge.PRODUCTID
                END NODES
                    LABEL SET (Order)
                    FROM VIEW_ORDERS end_nodes
                        JOIN ON end_nodes.ORDERID = edge.ORDERID,

   			 -- (OrderDetails)-[HAS_PRODUCT]->(Product)
        (HAS_PRODUCT)
            FROM VIEW_ORDER_DETAILS edge
                START NODES
                    LABEL SET (OrderDetails)
                    -- edge table is also start table, each row joining to itself
                    FROM VIEW_ORDER_DETAILS start_nodes
                        JOIN ON start_nodes.ORDERID = edge.ORDERID                        
                        AND start_nodes.PRODUCTID = edge.PRODUCTID
                END NODES
                    LABEL SET (Product)
                    FROM VIEW_PRODUCTS end_nodes
                        JOIN ON end_nodes.PRODUCTID = edge.PRODUCTID,

    		-- (Order)-[HAS_SHIPPER]->(Shipper)
        (HAS_SHIPPER)
            FROM VIEW_ORDERS edge
                START NODES
                    LABEL SET (Order)
                    -- edge table is also start table, each row joining to itself
                    FROM VIEW_ORDERS start_nodes
                        JOIN ON start_nodes.ORDERID = edge.ORDERID
                END NODES
                    LABEL SET (Shipper)
                    FROM VIEW_SHIPPERS end_nodes
                        JOIN ON end_nodes.SHIPPERID = edge.SHIPVIA,

				-- (:Product)-[:HAS_SUPPLIER]->(:Supplier)
        (HAS_SUPPLIER)
            FROM VIEW_PRODUCTS edge
                START NODES
                    LABEL SET (Product)
                    -- edge table is also start table, each row joining to itself
                    FROM VIEW_PRODUCTS start_nodes
                        JOIN ON start_nodes.PRODUCTID = edge.PRODUCTID
                END NODES
                    LABEL SET (Supplier)
                    FROM VIEW_SUPPLIERS end_nodes
                        JOIN ON end_nodes.SUPPLIERID = edge.SUPPLIERID,

		    -- (:Employee)-[:REPORTS_TO]->(:Employee)
        (REPORTS_TO)
            FROM VIEW_EMPLOYEES edge
                START NODES
                    LABEL SET (Employee)
                    -- edge table is also start table, each row joining to itself
                    FROM VIEW_EMPLOYEES start_nodes
                        JOIN ON start_nodes.EMPLOYEEID = edge.EMPLOYEEID
                END NODES
                    LABEL SET (Employee)
                    -- edge table is also start table, each row joining to itself
                    FROM VIEW_EMPLOYEES end_nodes
                        JOIN ON end_nodes.REPORTSTO = edge.REPORTSTO
    )
)
