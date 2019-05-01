CREATE TABLE Employees
(
    EmployeeID number NOT NULL ,
    LastName varchar2 (20) NOT NULL ,
    FirstName varchar2 (10) NOT NULL ,
    Title varchar2 (30) NULL ,
    TitleOfCourtesy varchar2 (25) NULL ,
    BirthDate date NULL ,
    HireDate date NULL ,
    Address varchar2 (60) NULL ,
    City varchar2 (15) NULL ,
    Region varchar2 (15) NULL ,
    PostalCode varchar2 (10) NULL ,
    Country varchar2 (15) NULL ,
    HomePhone varchar2 (24) NULL ,
    Extension varchar2 (4) NULL ,
    Photo blob NULL ,
    Notes blob NULL ,
    ReportsTo number NULL ,
    PhotoPath varchar2 (255) NULL ,

    CONSTRAINT PK_Employees PRIMARY KEY
    (
        EmployeeID
    ),
    CONSTRAINT FK_Employee_ReportsTo FOREIGN KEY
    (
        ReportsTo
    ) REFERENCES Employees (
        EmployeeID
    ),

);

CREATE INDEX IDX_Employees_LastName ON Employees (LastName);

CREATE INDEX IDX_Employees_PostalCode ON Employees (PostalCode);

CREATE TABLE Categories
(
    CategoryID number NOT NULL ,
    CategoryName varchar2(15) NOT NULL ,
    Description varchar2(255) NULL ,
    Picture blob NULL ,

    CONSTRAINT PK_Categories PRIMARY KEY
    (
        CategoryID
    )
);

CREATE INDEX IDX_Categories_CategoryName ON Categories (CategoryName);

CREATE TABLE Customers
(
    CustomerID varchar2 (5) NOT NULL ,
    CompanyName varchar2 (40) NOT NULL ,
    ContactName varchar2 (30) NULL ,
    ContactTitle varchar2 (30) NULL ,
    Address varchar2 (60) NULL ,
    City varchar2 (25) NULL ,
    Region varchar2 (15) NULL ,
    PostalCode varchar2 (10) NULL ,
    Country varchar2 (15) NULL ,
    Phone varchar2 (24) NULL ,
    Fax varchar2 (24) NULL ,

    CONSTRAINT PK_Customers PRIMARY KEY
    (
        CustomerID
    )
);

CREATE INDEX IDX_Customers_City ON Customers (City);

CREATE INDEX IDX_Customers_CompanyName ON Customers (CompanyName);

CREATE INDEX IDX_Customers_PostalCode ON Customers (PostalCode);

CREATE INDEX IDX_Customers_Region ON Customers (Region);

CREATE TABLE Shippers
(
    ShipperID number NOT NULL ,
    CompanyName varchar2 (40) NOT NULL ,
    Phone varchar2 (24) NULL ,

    CONSTRAINT PK_Shippers PRIMARY KEY
    (
        ShipperID
    )
);

CREATE TABLE Suppliers
(
    SupplierID number NOT NULL ,
    CompanyName varchar2 (60) NOT NULL ,
    ContactName varchar2 (30) NULL ,
    ContactTitle varchar2 (30) NULL ,
    Address varchar2 (60) NULL ,
    City varchar2 (15) NULL ,
    Region varchar2 (15) NULL ,
    PostalCode varchar2 (10) NULL ,
    Country varchar2 (15) NULL ,
    Phone varchar2 (24) NULL ,
    Fax varchar2 (24) NULL ,
    HomePage varchar2(2000) NULL ,

    CONSTRAINT PK_Suppliers PRIMARY KEY
    (
        SupplierID
    )
);

CREATE INDEX IDX_Suppliers_CompanyName ON Suppliers (CompanyName);

CREATE INDEX IDX_Suppliers_PostalCode ON Suppliers (PostalCode);

CREATE TABLE Orders
(
    OrderID number NOT NULL ,
    CustomerID varchar2 (5) NULL ,
    EmployeeID number NULL ,
    OrderDate varchar2(15) NULL ,
    RequiredDate varchar2(15) NULL ,
    ShippedDate varchar2(15) NULL ,
    ShipVia number NULL ,
    Freight number NULL,
    ShipName varchar2 (40) NULL ,
    ShipAddress varchar2 (60) NULL ,
    ShipCity varchar2 (25) NULL ,
    ShipRegion varchar2 (15) NULL ,
    ShipPostalCode varchar2 (10) NULL ,
    ShipCountry varchar2 (15) NULL ,

    CONSTRAINT PK_Orders PRIMARY KEY
    (
        OrderID
    ),
    CONSTRAINT FK_Orders_Customers FOREIGN KEY
    (
        CustomerID
    ) REFERENCES Customers (
        CustomerID
    ),
    CONSTRAINT FK_Orders_Employees FOREIGN KEY
    (
        EmployeeID
    ) REFERENCES Employees (
        EmployeeID
    ),
    CONSTRAINT FK_Orders_Shippers FOREIGN KEY
    (
        ShipVia
    ) REFERENCES Shippers (
        ShipperID
    )
);

CREATE INDEX IDX_Orders_CustomerID ON Orders (CustomerID);

CREATE INDEX IDX_Orders_EmployeeID ON Orders (EmployeeID);

CREATE INDEX IDX_Orders_OrderDate ON Orders (OrderDate);

CREATE INDEX IDX_Orders_ShippedDate ON Orders (ShippedDate);

CREATE INDEX IDX_Orders_ShippersOrders ON Orders (ShipVia);

CREATE INDEX IDX_Orders_ShipPostalCode ON Orders (ShipPostalCode);

CREATE TABLE Products
(
    ProductID number  NOT NULL ,
    ProductName varchar2 (60) NOT NULL ,
    SupplierID number NULL ,
    CategoryID number NULL ,
    QuantityPerUnit varchar2 (20) NULL ,
    UnitPrice number NULL ,
    UnitsInStock number NULL ,
    UnitsOnOrder number NULL ,
    ReorderLevel number NULL ,
    Discontinued number NOT NULL ,

    CONSTRAINT PK_Products PRIMARY KEY
    (
        ProductID
    ),
    CONSTRAINT FK_Products_Categories FOREIGN KEY
    (
        CategoryID
    ) REFERENCES Categories (
        CategoryID
    ),
    CONSTRAINT FK_Products_Suppliers FOREIGN KEY
    (
        SupplierID
    ) REFERENCES Suppliers (
        SupplierID
    )
);

CREATE INDEX IDX_Products_CategoryID ON Products (CategoryID);

CREATE INDEX IDX_Products_ProductName ON Products (ProductName);

CREATE INDEX IDX_Products_SupplierID ON Products (SupplierID);

CREATE TABLE Order_Details
(
    OrderID number NOT NULL ,
    ProductID number NOT NULL ,
    UnitPrice number NOT NULL ,
    Quantity number NOT NULL ,
    Discount number NOT NULL ,

    CONSTRAINT PK_Order_Details PRIMARY KEY
    (
        OrderID,
        ProductID
    ),
    CONSTRAINT FK_Order_Details_Orders FOREIGN KEY
    (
        OrderID
    ) REFERENCES Orders (
        OrderID
    ),
    CONSTRAINT FK_Order_Details_Products FOREIGN KEY
    (
        ProductID
    ) REFERENCES Products (
        ProductID
    )
);

CREATE INDEX IDX_Order_Details_OrderID ON Order_Details (OrderID);

CREATE INDEX IDX_Order_Details_ProductID ON Order_Details (ProductID);

CREATE TABLE CustomerCustomerDemo
(
    CustomerID varchar2(5) NOT NULL ,
    CustomerTypeID varchar2(10) NOT NULL ,

    CONSTRAINT PK_CustomerCustomerDemo PRIMARY KEY
    (
        CustomerID,
        CustomerTypeID
    )
);

CREATE TABLE CustomerDemographics
(
    CustomerTypeID varchar2 (10) NOT NULL ,
    CustomerDesc varchar2(200) NULL ,

    CONSTRAINT PK_CustomerDemographics PRIMARY KEY
    (
        CustomerTypeID
    )
);

CREATE TABLE Region
(
    RegionID number NOT NULL ,
    RegionDescription varchar2 (50) NOT NULL ,

    CONSTRAINT PK_Region PRIMARY KEY
    (
        RegionID
    )
);

CREATE TABLE Territories
(
    TerritoryID varchar2 (20) NOT NULL ,
    TerritoryDescription varchar2 (50) NOT NULL ,
    RegionID number NOT NULL ,

    CONSTRAINT PK_Territories PRIMARY KEY
    (
        TerritoryID
    ),
    CONSTRAINT FK_Territories_Region FOREIGN KEY
    (
        RegionID
    ) REFERENCES Region
    (
        RegionID
    )
);

CREATE TABLE EmployeeTerritories
(
    EmployeeID number NOT NULL ,
    TerritoryID varchar2 (20) NOT NULL ,

    CONSTRAINT PK_EmployeeTerritories PRIMARY KEY
    (
        EmployeeID,
        TerritoryID
    ),
    CONSTRAINT FK_EmployeeTerritories_Empl FOREIGN KEY
    (
        EmployeeID
    ) REFERENCES Employees
    (
        EmployeeID
    ),
    CONSTRAINT FK_EmployeeTerritories_Terr FOREIGN KEY
    (
        TerritoryID
    ) REFERENCES Territories
    (
        TerritoryID
    )
);
