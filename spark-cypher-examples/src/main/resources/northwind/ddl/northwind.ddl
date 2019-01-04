-- format for below is: <dataSourceName>.<schemaName>
SET SCHEMA H2.NORTHWIND;

CREATE GRAPH Northwind (

  -- Nodes
  Employee (
    employeeID INTEGER,
    lastName STRING,
    firstName STRING,
    title STRING?,
    titleOfCourtesy STRING?,
    birthDate STRING?,
    hireDate STRING?,
    address STRING?,
    city STRING?,
    region STRING?,
    postalCode STRING?,
    country STRING?,
    homePhone STRING?,
    extension STRING?,
    reportsTo INTEGER?,
    photoPath STRING?
  ),

  Territory (
    territoryID STRING,
    territoryDescription STRING,
    regionID INTEGER
  ),

  Supplier (
    supplierID INTEGER,
    companyName STRING,
    contactName STRING?,
    contactTitle STRING?,
    address STRING?,
    city STRING?,
    region STRING?,
    postalCode STRING?,
    country STRING?,
    phone STRING?,
    fax STRING?,
    homePage STRING?
  ),

  Customer (
    customerID STRING,
    companyName STRING,
    contactName STRING?,
    contactTitle STRING?,
    address STRING?,
    city STRING?,
    region STRING?,
    postalCode STRING?,
    country STRING?,
    phone STRING?,
    fax STRING?
  ),

  Product (
    productID INTEGER,
    productName STRING,
    supplierID INTEGER?,
    categoryID INTEGER?,
    quantityPerUnit STRING?,
    unitPrice INTEGER?,
    unitsInStock INTEGER?,
    unitsOnOrder INTEGER?,
    reorderLevel INTEGER?,
    discontinued INTEGER
  ),

  OrderDetails (
    orderID INTEGER,
    productID INTEGER,
    unitPrice INTEGER,
    quantity INTEGER,
    discount INTEGER
  ),

  Category (
    categoryID INTEGER,
    categoryName STRING,
    description STRING?
  ),

  Region (
    regionID INTEGER,
    regionDescription STRING
  ),

  Order (
    orderID INTEGER,
    customerID STRING?,
    employeeID INTEGER?,
    orderDate STRING?,
    requiredDate STRING?,
    shippedDate STRING?,
    shipVia INTEGER?,
    freight INTEGER?,
    shipName STRING?,
    shipAddress STRING?,
    shipCity STRING?,
    shipRegion STRING?,
    shipPostalCode STRING?,
    shipCountry STRING?
  ),

  Shipper (
    shipperID INTEGER,
    companyName STRING,
    phone STRING?
  ),

  CustomerDemographic (
    customerTypeID STRING,
    customerDesc STRING?
  ),

  -- Relationships
  HAS_SUPPLIER,
  HAS_PRODUCT,
  HAS_CATEGORY,
  HAS_TERRITORY,
  HAS_EMPLOYEE,
  REPORTS_TO,
  HAS_CUSTOMER,
  HAS_CUSTOMER_DEMOGRAPHIC,
  HAS_ORDER,
  HAS_SHIPPER,
  HAS_REGION,

  -- Node mappings
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
       FROM VIEW_SUPPLIERS,

  -- Relationship mappings
  (Product)-[HAS_CATEGORY]->(Category)
      FROM VIEW_PRODUCTS edge
          START NODES (Product)
              FROM VIEW_PRODUCTS start_nodes
                  JOIN ON start_nodes.PRODUCTID = edge.PRODUCTID
          END NODES (Category)
              FROM VIEW_CATEGORIES end_nodes
                  JOIN ON end_nodes.CATEGORYID = edge.CATEGORYID,

  (Territory)-[HAS_REGION]->(Region)
      FROM VIEW_TERRITORIES edge
          START NODES (Territory)
              FROM VIEW_TERRITORIES start_nodes
                  JOIN ON start_nodes.TERRITORYID = edge.TERRITORYID
          END NODES (Region)
              FROM VIEW_REGION end_nodes
                  JOIN ON end_nodes.REGIONID = edge.REGIONID,

  (Employee)-[HAS_TERRITORY]->(Territory)
      FROM VIEW_EMPLOYEETERRITORIES edge
          START NODES (Employee)
              FROM VIEW_EMPLOYEES start_nodes
                  JOIN ON start_nodes.EMPLOYEEID = edge.EMPLOYEEID
          END NODES (Territory)
              FROM VIEW_TERRITORIES end_nodes
                  JOIN ON end_nodes.TERRITORYID = edge.TERRITORYID,

  (Territory)-[HAS_EMPLOYEE]->(Employee)
       FROM VIEW_EMPLOYEETERRITORIES edge
          START NODES (Territory)
              FROM VIEW_TERRITORIES start_nodes
                  JOIN ON start_nodes.TERRITORYID = edge.TERRITORYID
          END NODES (Employee)
              FROM VIEW_EMPLOYEES end_nodes
                  JOIN ON end_nodes.EMPLOYEEID = edge.EMPLOYEEID,

   (Order)-[HAS_EMPLOYEE]->(Employee)
       FROM VIEW_ORDERS edge
           START NODES (Order)
               FROM VIEW_ORDERS start_nodes
                   JOIN ON start_nodes.ORDERID = edge.ORDERID
           END NODES (Employee)
               FROM VIEW_EMPLOYEES end_nodes
                   JOIN ON end_nodes.EMPLOYEEID = edge.EMPLOYEEID,

  (Customer)-[HAS_CUSTOMER_DEMOGRAPHIC]->(CustomerDemographic)
      FROM CUSTOMERCUSTOMERDEMO edge
          START NODES (Customer)
              FROM VIEW_CUSTOMERS start_nodes
                  JOIN ON start_nodes.CUSTOMERID = edge.CUSTOMERID
          END NODES (CustomerDemographic)
              FROM VIEW_CUSTOMERDEMOGRAPHICS end_nodes
                  JOIN ON end_nodes.CUSTOMERTYPEID = edge.CUSTOMERTYPEID,

  (Order)-[HAS_CUSTOMER]->(Customer)
      FROM VIEW_ORDERS edge
          START NODES (Order)
              FROM VIEW_ORDERS start_nodes
                  JOIN ON start_nodes.ORDERID = edge.ORDERID
          END NODES (Customer)
              FROM VIEW_CUSTOMERS end_nodes
                  JOIN ON end_nodes.CUSTOMERID = edge.CUSTOMERID,

  (CustomerDemographic)-[HAS_CUSTOMER]->(Customer)
      FROM CUSTOMERCUSTOMERDEMO edge
          START NODES (CustomerDemographic)
              FROM VIEW_CUSTOMERDEMOGRAPHICS start_nodes
                  JOIN ON start_nodes.CUSTOMERTYPEID = edge.CUSTOMERTYPEID
          END NODES (Customer)
              FROM VIEW_CUSTOMERS end_nodes
                  JOIN ON end_nodes.CUSTOMERID = edge.CUSTOMERID,

  (OrderDetails)-[HAS_ORDER]->(Order)
      FROM VIEW_ORDER_DETAILS edge
          START NODES (OrderDetails)
              FROM VIEW_ORDER_DETAILS start_nodes
                  JOIN ON start_nodes.ORDERID = edge.ORDERID
                  AND start_nodes.PRODUCTID = edge.PRODUCTID
          END NODES (Order)
              FROM VIEW_ORDERS end_nodes
                  JOIN ON end_nodes.ORDERID = edge.ORDERID,

  (OrderDetails)-[HAS_PRODUCT]->(Product)
      FROM VIEW_ORDER_DETAILS edge
          START NODES (OrderDetails)
              FROM VIEW_ORDER_DETAILS start_nodes
                  JOIN ON start_nodes.ORDERID = edge.ORDERID
                  AND start_nodes.PRODUCTID = edge.PRODUCTID
          END NODES (Product)
              FROM VIEW_PRODUCTS end_nodes
                  JOIN ON end_nodes.PRODUCTID = edge.PRODUCTID,

  (Order)-[HAS_SHIPPER]->(Shipper)
      FROM VIEW_ORDERS edge
          START NODES (Order)
              FROM VIEW_ORDERS start_nodes
                  JOIN ON start_nodes.ORDERID = edge.ORDERID
          END NODES (Shipper)
              FROM VIEW_SHIPPERS end_nodes
                  JOIN ON end_nodes.SHIPPERID = edge.SHIPVIA,

  (Product)-[HAS_SUPPLIER]->(Supplier)
      FROM VIEW_PRODUCTS edge
          START NODES (Product)
              FROM VIEW_PRODUCTS start_nodes
                  JOIN ON start_nodes.PRODUCTID = edge.PRODUCTID
          END NODES (Supplier)
              FROM VIEW_SUPPLIERS end_nodes
                  JOIN ON end_nodes.SUPPLIERID = edge.SUPPLIERID,

  (Employee)-[REPORTS_TO]->(Employee)
      FROM VIEW_EMPLOYEES edge
          START NODES (Employee)
              FROM VIEW_EMPLOYEES start_nodes
                  JOIN ON start_nodes.EMPLOYEEID = edge.EMPLOYEEID
          END NODES (Employee)
              FROM VIEW_EMPLOYEES end_nodes
                  JOIN ON end_nodes.REPORTSTO = edge.REPORTSTO
)
