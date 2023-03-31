
-- select count(*) from categories;
-- select count(*) from customers;
-- select count(*) from employees;
-- select count(*) from orderdetails;
-- select count(*) from products;
-- select count(*) from northwind2.products;
-- select count(*) from northwind.shippers;

USE `northwind2`;


-- 1. Order Subtotals
-- For each order, calculate a subtotal for each Order (identified by OrderID). This is a simple query using GROUP BY to aggregate data for each order.

SELECT 
    OrderID,
    FORMAT(SUM(UnitPrice * Quantity * (1 - Discount)),
        2) AS Subtotal
FROM
    order_details
GROUP BY OrderID
ORDER BY OrderID;

-- 2. Sales by Year
-- This query shows how to get the year part from Shipped_Date column. A subtotal is calculated by a sub-query for each order. The sub-query forms a table and then joined with the Orders table.
SELECT DISTINCT
    DATE(a.ShippedDate) AS ShippedDate,
    a.OrderID,
    b.Subtotal,
    YEAR(a.ShippedDate) AS Year
FROM
    Orders a
        INNER JOIN
    (SELECT DISTINCT
        OrderID,
            FORMAT(SUM(UnitPrice * Quantity * (1 - Discount)), 2) AS Subtotal
    FROM
        order_details
    GROUP BY OrderID) b ON a.OrderID = b.OrderID
WHERE
    a.ShippedDate IS NOT NULL
        AND a.ShippedDate BETWEEN DATE('1996-12-24') AND DATE('1997-09-30')
ORDER BY ShippedDate;


-- 3. Employee Sales by Country
-- For each employee, get their sales amount, broken down by country name.

-- 4. Alphabetical List of Products
-- This is a rather simple query to get an alphabetical list of products.


SELECT DISTINCT
    b.*, a.CategoryName
FROM
    Categories a
        INNER JOIN
    Products b ON a.CategoryID = b.CategoryID
WHERE
    b.Discontinued = 'N'
ORDER BY b.ProductName
;

-- 5. Current Product List
-- This is another simple query. No aggregation is used for summarizing data.

SELECT 
    ProductID, ProductName
FROM
    products
WHERE
    Discontinued = 'N'
ORDER BY ProductName;

-- 6. Order Details Extended
-- This query calculates sales price for each order after discount is applied.

SELECT DISTINCT
    y.OrderID,
    y.ProductID,
    x.ProductName,
    y.UnitPrice,
    y.Quantity,
    y.Discount,
    ROUND(y.UnitPrice * y.Quantity * (1 - y.Discount),
            2) AS ExtendedPrice
FROM
    Products x
        INNER JOIN
    Order_Details y ON x.ProductID = y.ProductID
ORDER BY y.OrderID;

-- 7. Sales by Category


SELECT DISTINCT
    a.CategoryID,
    a.CategoryName,
    b.ProductName,
    SUM(ROUND(y.UnitPrice * y.Quantity * (1 - y.Discount),
            2)) AS ProductSales
FROM
    Order_Details y
        INNER JOIN
    Orders d ON d.OrderID = y.OrderID
        INNER JOIN
    Products b ON b.ProductID = y.ProductID
        INNER JOIN
    Categories a ON a.CategoryID = b.CategoryID
WHERE
    d.OrderDate BETWEEN DATE('1997/1/1') AND DATE('1997/12/31')
GROUP BY a.CategoryID , a.CategoryName , b.ProductName
ORDER BY a.CategoryName , b.ProductName , ProductSales
;

-- 8. Ten Most Expensive Products

SELECT DISTINCT
    ProductName AS Ten_Most_Expensive_Products, UnitPrice
FROM
    Products AS a
WHERE
    10 >= (SELECT 
            COUNT(DISTINCT UnitPrice)
        FROM
            Products AS b
        WHERE
            b.UnitPrice >= a.UnitPrice)
ORDER BY UnitPrice DESC;

-- 9. Products by Category

SELECT DISTINCT
    a.CategoryName,
    b.ProductName,
    b.QuantityPerUnit,
    b.UnitsInStock,
    b.Discontinued
FROM
    Categories a
        INNER JOIN
    Products b ON a.CategoryID = b.CategoryID
WHERE
    b.Discontinued = 'N'
ORDER BY a.CategoryName , b.ProductName;


-- 10. Customers and Suppliers by City
-- This query shows how to use UNION to merge Customers and Suppliers into one result set by identifying them as having different relationships to Northwind Traders - Customers and Suppliers.

SELECT 
    City, CompanyName, ContactName, 'Customers' AS Relationship
FROM
    Customers 
UNION SELECT 
    City, CompanyName, ContactName, 'Suppliers'
FROM
    Suppliers
ORDER BY City , CompanyName;

SELECT 
	City, CompanyName, ContactName, 'Suppliers'
FROM
	Suppliers;


-- 11. Products Above Average Price
-- This query shows how to use sub-query to get a single value (average unit price) that can be used in the outer-query.

SELECT DISTINCT
    ProductName, UnitPrice
FROM
    Products
WHERE
    UnitPrice > (SELECT 
            AVG(UnitPrice)
        FROM
            Products)
ORDER BY UnitPrice;

-- 12. Product Sales for 1997
-- This query shows how to group categories and products by quarters and shows sales amount for each quarter.

SELECT DISTINCT
    a.CategoryName,
    b.ProductName,
    FORMAT(SUM(c.UnitPrice * c.Quantity * (1 - c.Discount)),
        2) AS ProductSales,
    CONCAT('Qtr ', QUARTER(d.ShippedDate)) AS ShippedQuarter
FROM
    Categories a
        INNER JOIN
    Products b ON a.CategoryID = b.CategoryID
        INNER JOIN
    Order_Details c ON b.ProductID = c.ProductID
        INNER JOIN
    Orders d ON d.OrderID = c.OrderID
WHERE
    d.ShippedDate BETWEEN DATE('1997-01-01') AND DATE('1997-12-31')
GROUP BY a.CategoryName , b.ProductName , CONCAT('Qtr ', QUARTER(d.ShippedDate))
ORDER BY a.CategoryName , b.ProductName , ShippedQuarter;
    
-- 13. Category Sales for 1997
-- This query shows sales figures by categories - mainly just aggregation with sub-query. The inner query aggregates to product level, and the outer query further aggregates the result set from inner-query to category level.

SELECT 
    CategoryName, FORMAT(SUM(ProductSales), 2) AS CategorySales
FROM
    (SELECT DISTINCT
        a.CategoryName,
            b.ProductName,
            FORMAT(SUM(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) AS ProductSales,
            CONCAT('Qtr ', QUARTER(d.ShippedDate)) AS ShippedQuarter
    FROM
        Categories AS a
    INNER JOIN Products AS b ON a.CategoryID = b.CategoryID
    INNER JOIN Order_Details AS c ON b.ProductID = c.ProductID
    INNER JOIN Orders AS d ON d.OrderID = c.OrderID
    WHERE
        d.ShippedDate BETWEEN DATE('1997-01-01') AND DATE('1997-12-31')
    GROUP BY a.CategoryName , b.ProductName , CONCAT('Qtr ', QUARTER(d.ShippedDate))
    ORDER BY a.CategoryName , b.ProductName , ShippedQuarter) AS x
GROUP BY CategoryName
ORDER BY CategoryName;

-- 14. Quarterly Orders by Product
-- This query shows how to convert order dates to the corresponding quarters. It also demonstrates how SUM function is used together with CASE statement to get sales for each quarter, where quarters are converted from OrderDate column.

SELECT 
    a.ProductName,
    d.CompanyName,
    YEAR(OrderDate) AS OrderYear,
    FORMAT(SUM(CASE QUARTER(c.OrderDate)
            WHEN '1' THEN b.UnitPrice * b.Quantity * (1 - b.Discount)
            ELSE 0
        END),
        0) 'Qtr 1',
    FORMAT(SUM(CASE QUARTER(c.OrderDate)
            WHEN '2' THEN b.UnitPrice * b.Quantity * (1 - b.Discount)
            ELSE 0
        END),
        0) 'Qtr 2',
    FORMAT(SUM(CASE QUARTER(c.OrderDate)
            WHEN '3' THEN b.UnitPrice * b.Quantity * (1 - b.Discount)
            ELSE 0
        END),
        0) 'Qtr 3',
    FORMAT(SUM(CASE QUARTER(c.OrderDate)
            WHEN '4' THEN b.UnitPrice * b.Quantity * (1 - b.Discount)
            ELSE 0
        END),
        0) 'Qtr 4'
FROM
    Products a
        INNER JOIN
    Order_Details b ON a.ProductID = b.ProductID
        INNER JOIN
    Orders c ON c.OrderID = b.OrderID
        INNER JOIN
    Customers d ON d.CustomerID = c.CustomerID
WHERE
    c.OrderDate BETWEEN DATE('1997-01-01') AND DATE('1997-12-31')
GROUP BY a.ProductName , d.CompanyName , YEAR(OrderDate)
ORDER BY a.ProductName , d.CompanyName;
 
-- 15. Invoice
-- A simple query to get detailed information for each sale so that invoice can be issued.
SELECT DISTINCT
    b.ShipName,
    b.ShipAddress,
    b.ShipCity,
    b.ShipRegion,
    b.ShipPostalCode,
    b.ShipCountry,
    b.CustomerID,
    c.CompanyName,
    c.Address,
    c.City,
    c.Region,
    c.PostalCode,
    c.Country,
    CONCAT(d.FirstName, ' ', d.LastName) AS Salesperson,
    b.OrderID,
    b.OrderDate,
    b.RequiredDate,
    b.ShippedDate,
    a.CompanyName,
    e.ProductID,
    f.ProductName,
    e.UnitPrice,
    e.Quantity,
    e.Discount,
    e.UnitPrice * e.Quantity * (1 - e.Discount) AS ExtendedPrice,
    b.Freight
FROM
    Shippers a
        INNER JOIN
    Orders b ON a.ShipperID = b.ShipVia
        INNER JOIN
    Customers c ON c.CustomerID = b.CustomerID
        INNER JOIN
    Employees d ON d.EmployeeID = b.EmployeeID
        INNER JOIN
    Order_Details e ON b.OrderID = e.OrderID
        INNER JOIN
    Products f ON f.ProductID = e.ProductID
ORDER BY b.ShipName;