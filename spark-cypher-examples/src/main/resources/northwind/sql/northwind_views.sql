CREATE VIEW view_Employees AS
SELECT convert(EmployeeID, bigint) AS EmployeeID,
    LastName,
    FirstName,
    Title,
    TitleOfCourtesy,
    convert(BirthDate, VARCHAR(10)) AS BirthDate,
    convert(HireDate, VARCHAR(10)) AS HireDate,
    Address,
    City,
    Region,
    PostalCode,
    Country,
    HomePhone,
    Extension,
    convert(ReportsTo, bigint) AS ReportsTo,
    PhotoPath
FROM Employees;

CREATE VIEW view_Categories AS
SELECT convert(CategoryID, bigint) AS CategoryID,
    CategoryName,
    Description
FROM Categories;

CREATE VIEW view_Shippers AS
SELECT convert(ShipperID, bigint) AS ShipperID,
    CompanyName,
    Phone
FROM Shippers;

CREATE VIEW view_Suppliers AS
SELECT convert(SupplierID, bigint) AS SupplierID,
    CompanyName,
    ContactName,
    ContactTitle,
    Address,
    City,
    Region,
    PostalCode,
    Country,
    Phone,
    Fax,
    HomePage
FROM Suppliers;

CREATE VIEW view_Orders AS
SELECT convert(OrderID, bigint) AS OrderID,
    CustomerID,
    convert(EmployeeID, bigint) AS EmployeeID,
    OrderDate,
    RequiredDate,
    ShippedDate,
    convert(ShipVia, bigint) AS ShipVia,
    convert(Freight, bigint) AS Freight,
    ShipName,
    ShipAddress,
    ShipCity,
    ShipRegion,
    ShipPostalCode,
    ShipCountry
FROM Orders;

CREATE VIEW view_Products AS
SELECT convert(ProductID, bigint) AS ProductID,
    ProductName,
    convert(SupplierID, bigint) AS SupplierID,
    convert(CategoryID, bigint) AS CategoryID,
    QuantityPerUnit,
    convert(UnitPrice, bigint) AS UnitPrice,
    convert(UnitsInStock, bigint) AS UnitsInStock,
    convert(UnitsOnOrder, bigint) AS UnitsOnOrder,
    convert(ReorderLevel, bigint) AS ReorderLevel,
    convert(Discontinued, bigint) AS Discontinued
FROM Products;

CREATE VIEW view_Order_Details AS
SELECT convert(OrderID, bigint) AS OrderID,
    convert(ProductID, bigint) AS ProductID,
    convert(UnitPrice, bigint) AS UnitPrice,
    convert(Quantity, bigint) AS Quantity,
    convert(Discount, bigint) AS Discount
FROM Order_Details;

CREATE VIEW view_Region AS
SELECT convert(RegionID, bigint) AS RegionID,
  RegionDescription
FROM Region;

CREATE VIEW view_Territories AS
SELECT TerritoryID,
    TerritoryDescription,
    convert(RegionID, bigint) AS RegionID
FROM Territories;

CREATE VIEW view_EmployeeTerritories AS
SELECT convert(EmployeeID, bigint) AS EmployeeID,
    TerritoryID
FROM EmployeeTerritories;

CREATE VIEW view_Customers AS
SELECT CustomerID,
    CompanyName,
    ContactName,
    ContactTitle,
    Address,
    City,
    Region,
    PostalCode,
    Country,
    Phone,
    Fax
FROM Customers;

CREATE VIEW view_CustomerDemographics AS
SELECT CustomerTypeID,
    CustomerDesc
FROM CustomerDemographics;
