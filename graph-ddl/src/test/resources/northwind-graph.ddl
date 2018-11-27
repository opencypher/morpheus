-- format for below is: <dataSourceName>.<schemaName>
SET SCHEMA H2.NORTHWIND;

-- Node labels

CREATE ELEMENT TYPE Employee ({
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

CREATE ELEMENT TYPE Territory ({
  territoryID: STRING,
  territoryDescription: STRING,
  regionID: INTEGER
})

CREATE ELEMENT TYPE Supplier ({
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

CREATE ELEMENT TYPE Customer ({
  customerID: INTEGER,
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

CREATE ELEMENT TYPE Product ({
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

CREATE ELEMENT TYPE OrderDetails ({
  orderID: INTEGER,
  productID: INTEGER,
  unitPrice: INTEGER,
  quantity: INTEGER,
  discount: INTEGER
})

CREATE ELEMENT TYPE Category ({
  categoryID: INTEGER,
  categoryName: STRING,
  description: STRING?
})

CREATE ELEMENT TYPE Region ({
  regionID: INTEGER,
  regionDescription: STRING
})

CREATE ELEMENT TYPE Order ({
  orderID: INTEGER,
  customerID: INTEGER?,
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

CREATE ELEMENT TYPE Shipper ({
  shipperID: INTEGER,
  companyName: STRING,
  phone: STRING?
})

CREATE ELEMENT TYPE CustomerDemographic ({
  customerTypeID: STRING,
  customerDesc: STRING?
})

-- Relationship types

CREATE ELEMENT TYPE HAS_SUPPLIER
CREATE ELEMENT TYPE HAS_PRODUCT
CREATE ELEMENT TYPE HAS_CATEGORY
CREATE ELEMENT TYPE HAS_TERRITORY
CREATE ELEMENT TYPE HAS_EMPLOYEE
CREATE ELEMENT TYPE REPORTS_TO
CREATE ELEMENT TYPE HAS_CUSTOMER
CREATE ELEMENT TYPE HAS_CUSTOMER_DEMOGRAPHIC
CREATE ELEMENT TYPE HAS_ORDER
CREATE ELEMENT TYPE HAS_SHIPPER
CREATE ELEMENT TYPE HAS_REGION

-- =================================================================

CREATE GRAPH TYPE NORTHWIND_NAIVE (

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
CREATE GRAPH Northwind OF NORTHWIND_NAIVE (
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
       FROM VIEW_SUPPLIER,

  -- (Product)-[HAS_CATEGORY]->(Category)
  [HAS_CATEGORY]
      FROM VIEW_PRODUCTS edge
          START NODES (Product)
              FROM VIEW_PRODUCTS start_nodes
                  JOIN ON start_nodes.PRODUCTID = edge.PRODUCTID
          END NODES (Category)
              FROM VIEW_CATEGORIES end_nodes
                  JOIN ON end_nodes.CATEGORYID = edge.CATEGORYID,

  -- (Territory)-[HAS_REGION]->(Region)
  [HAS_REGION]
      FROM VIEW_TERRITORIES edge
          START NODES (Territory)
              FROM VIEW_TERRITORIES start_nodes
                  JOIN ON start_nodes.TERRITORYID = edge.TERRITORYID
          END NODES (Region)
              FROM VIEW_REGION end_nodes
                  JOIN ON end_nodes.REGIONID = edge.REGIONID,

  -- (:Employee)-[:HAS_TERRITORY]->(:Territory)
  [HAS_TERRITORY]
      FROM VIEW_EMPLOYEETERRITORIES edge
          START NODES (Employee)
              FROM VIEW_EMPLOYEES start_nodes
                  JOIN ON start_nodes.EMPLOYEEID = edge.EMPLOYEEID
          END NODES (Territory)
              FROM VIEW_TERRITORIES end_nodes
                  JOIN ON end_nodes.TERRITORYID = edge.TERRITORYID,

  [HAS_EMPLOYEE]
       -- (:Territory)-[:HAS_EMPLOYEE]->(:Employee)
       FROM VIEW_EMPLOYEETERRITORIES edge
          START NODES (Territory)
              FROM VIEW_TERRITORIES start_nodes
                  JOIN ON start_nodes.TERRITORYID = edge.TERRITORYID
          END NODES (Employee)
              FROM VIEW_EMPLOYEES end_nodes
                  JOIN ON end_nodes.EMPLOYEEID = edge.EMPLOYEEID,
   -- (:Order)-[:HAS_EMPLOYEE]->(:Employee)
       FROM VIEW_ORDERS edge
           START NODES (Order)
               FROM VIEW_ORDERS start_nodes
                   JOIN ON start_nodes.ORDERID = edge.ORDERID
           END NODES (Employee)
               FROM VIEW_EMPLOYEES end_nodes
                   JOIN ON end_nodes.EMPLOYEEID = edge.EMPLOYEEID,

  -- (Customer)-[HAS_CUSTOMER_DEMOGRAPHIC]->(CustomerDemographic)
  [HAS_CUSTOMER_DEMOGRAPHIC]
      FROM CUSTOMERCUSTOMERDEMO edge
          START NODES (Customer)
              FROM VIEW_CUSTOMERS start_nodes
                  JOIN ON start_nodes.CUSTOMERID = edge.CUSTOMERID
          END NODES (CustomerDemographic)
              FROM VIEW_CUSTOMERDEMOGRAPHICS end_nodes
                  JOIN ON end_nodes.CUSTOMERTYPEID = edge.CUSTOMERTYPEID,

  [HAS_CUSTOMER]
      -- (Order)-[HAS_CUSTOMER]->(Customer)
      FROM VIEW_ORDERS edge
          START NODES (Order)
              FROM VIEW_ORDERS start_nodes
                  JOIN ON start_nodes.ORDERID = edge.ORDERID
          END NODES (Customer)
              FROM VIEW_CUSTOMERS end_nodes
                  JOIN ON end_nodes.CUSTOMERID = edge.CUSTOMERID,

      -- (CustomerDemographic)-[HAS_CUSTOMER]->(Customer)
      FROM CUSTOMERCUSTOMERDEMO edge
          START NODES (CustomerDemographic)
              FROM VIEW_CUSTOMERDEMOGRAPHICS start_nodes
                  JOIN ON start_nodes.CUSTOMERTYPEID = edge.CUSTOMERTYPEID
          END NODES (Customer)
              FROM VIEW_CUSTOMERS end_nodes
                  JOIN ON end_nodes.CUSTOMERID = edge.CUSTOMERID,

  -- (OrderDetails)-[HAS_ORDER]->(Order)
  [HAS_ORDER]
      FROM VIEW_ORDER_DETAILS edge
          START NODES (OrderDetails)
              FROM VIEW_ORDER_DETAILS start_nodes
                  JOIN ON start_nodes.ORDERID = edge.ORDERID
                  AND start_nodes.PRODUCTID = edge.PRODUCTID
          END NODES (Order)
              FROM VIEW_ORDERS end_nodes
                  JOIN ON end_nodes.ORDERID = edge.ORDERID,

  -- (OrderDetails)-[HAS_PRODUCT]->(Product)
  [HAS_PRODUCT]
      FROM VIEW_ORDER_DETAILS edge
          START NODES (OrderDetails)
              FROM VIEW_ORDER_DETAILS start_nodes
                  JOIN ON start_nodes.ORDERID = edge.ORDERID
                  AND start_nodes.PRODUCTID = edge.PRODUCTID
          END NODES (Product)
              FROM VIEW_PRODUCTS end_nodes
                  JOIN ON end_nodes.PRODUCTID = edge.PRODUCTID,

  -- (Order)-[HAS_SHIPPER]->(Shipper)
  [HAS_SHIPPER]
      FROM VIEW_ORDERS edge
          START NODES (Order)
              FROM VIEW_ORDERS start_nodes
                  JOIN ON start_nodes.ORDERID = edge.ORDERID
          END NODES (Shipper)
              FROM VIEW_SHIPPERS end_nodes
                  JOIN ON end_nodes.SHIPPERID = edge.SHIPVIA,

  -- (:Product)-[:HAS_SUPPLIER]->(:Supplier)
  [HAS_SUPPLIER]
      FROM VIEW_PRODUCTS edge
          START NODES (Product)
              FROM VIEW_PRODUCTS start_nodes
                  JOIN ON start_nodes.PRODUCTID = edge.PRODUCTID
          END NODES (Supplier)
              FROM VIEW_SUPPLIERS end_nodes
                  JOIN ON end_nodes.SUPPLIERID = edge.SUPPLIERID,

  -- (:Employee)-[:REPORTS_TO]->(:Employee)
  [REPORTS_TO]
      FROM VIEW_EMPLOYEES edge
          START NODES (Employee)
              FROM VIEW_EMPLOYEES start_nodes
                  JOIN ON start_nodes.EMPLOYEEID = edge.EMPLOYEEID
          END NODES (Employee)
              FROM VIEW_EMPLOYEES end_nodes
                  JOIN ON end_nodes.REPORTSTO = edge.REPORTSTO
)
