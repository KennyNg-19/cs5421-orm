from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, inspect, MetaData, Table, func, distinct, desc, literal
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql import case
from memory_profiler import profile
import threading
import datetime
from concurrent.futures import ThreadPoolExecutor

# 创建对象的基类:
BaseModel = declarative_base()
database = 'northwind2'
user = 'root'
psw = '12345678'
# engine = create_engine(f"mysql+pymysql://{user}:{psw}@localhost:3306/{database}?charset=utf8", echo=True)  # 数据库链接驱动语句
# echo=True是开启调试，这样当我们执行文件的时候会提示相应的文字。
# 数据库连接驱动
engine = create_engine(f"mysql+pymysql://{user}:{psw}@localhost:3306/{database}?charset=utf8")  


# session用于创建程序和数据库之间的会话，所有对象的载入和保存都需要通过session对象 。
# 通过sessionmaker调用创建一个工厂，并关联Engine以确保每个session都可以使用该Engine连接资源：
DbSession = sessionmaker(bind=engine)  # 创建DBSession类: 连接池，连接选中数据库
session = DbSession()  # 打开查询窗口
# session的常见操作方法包括：
# flush：预提交，提交到数据库文件，还未写入数据库文件中
# commit：提交了一个事务
# rollback：回滚
# close：关闭


# 反向映射原生表到 Table类
Base = automap_base()
Base.prepare(engine, reflect=True)
# ['categories', 'customers', 'employees', 'order_details', 'orders', 'products', 'shippers', 'suppliers']
Categories = Base.classes.categories
Customers = Base.classes.customers
Employees = Base.classes.employees
Order_Details = Base.classes.order_details
Orders = Base.classes.orders
Products = Base.classes.products
Shippers = Base.classes.shippers
Suppliers = Base.classes.suppliers


def connect_databse():
    pass

def create_tables():
    # 利用 User 去数据库建立 user Table
    BaseModel.metadata.create_all(engine)  # 数据库引擎

def check_func():
    create_tables()
    
def sql_deemo():
    # 2.选择表
    # 3.建立查询窗口
    # 4.写入SQL语句
    Categories = Base.classes.categories
    res = session.query(Categories).filter(Categories.CategoryID == 1).all()
    for data in res:
        print(data.CategoryName)
    # 5.提交sql语句
    # db_session.commit()
    # 6.关闭查询窗口
    session.close()

def show_tables():
    insp = inspect(engine)
    out = insp.get_table_names(schema='northwind2')
    print(out)


# ---------------------------------------------------------------------

# @profile(precision=4,stream=open('memory_profiler.log','w+'))
def sql1():
    '''
    select OrderID, 
        format(sum(UnitPrice * Quantity * (1 - Discount)), 2) as Subtotal
    from order_details
    group by OrderID
    order by OrderID;
    '''
    # order_details = Order_Details
    session = DbSession()  # 打开查询窗口
    subtotal = func.format(func.sum(Order_Details.UnitPrice * Order_Details.Quantity * (1 - Order_Details.Discount)), 2).label('Subtotal')
    query = session.query(Order_Details.OrderID, subtotal).group_by(Order_Details.OrderID).order_by(Order_Details.OrderID)
    
    # query = session.query([order_details.OrderID, subtotal]).group_by(order_details.OrderID).order_by(order_details.OrderID)

    # for row in query:
    #     print(row)
    session.close()
    return query

# @profile()
def sql2():
    '''
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
            Order_Details
        GROUP BY OrderID) b ON a.OrderID = b.OrderID
    WHERE
        a.ShippedDate IS NOT NULL
            AND a.ShippedDate BETWEEN DATE('1996-12-24') AND DATE('1997-09-30')
    ORDER BY a.ShippedDate;
    '''
    session = DbSession()  # 打开查询窗口
    # 查询语句
    subtotal = func.sum(Order_Details.UnitPrice * Order_Details.Quantity * (1 - Order_Details.Discount)).label('Subtotal')
    sub_query = session.query(Order_Details.OrderID, subtotal).group_by(Order_Details.OrderID).subquery()

    query = (session.query(func.distinct(func.date(Orders.ShippedDate)).label('ShippedDate'),
                           Orders.OrderID,
                           sub_query.c.Subtotal,
                           func.year(Orders.ShippedDate).label('Year'))
             .join(sub_query, Orders.OrderID == sub_query.c.OrderID)
             .filter(Orders.ShippedDate.isnot(None), Orders.ShippedDate.between('1996-12-24', '1997-09-30'))
             .order_by("ShippedDate") # 光执行SQL本身 不ORM：在select之后了，所以必须 by alias ONLY, not by Orders.ShippedDate
                            # .order_by(Orders.ShippedDate)
            )

    # results = query.all()
    # for row in query:
    #     print(row)
    session.close()
    return query

# def sql3():
#     pass

def sql4():
    '''
    SELECT DISTINCT
        b.*, a.CategoryName
    FROM
        Categories a
            INNER JOIN
        Products b ON a.CategoryID = b.CategoryID
    WHERE
        b.Discontinued = 'N'
    ORDER BY b.ProductName;
    '''
    session = DbSession()  # 打开查询窗口

    query = (session.query(Products.__table__.c, Categories.CategoryName)
            .join(Categories, Categories.CategoryID == Products.CategoryID)
            .filter(Products.Discontinued == 'N')
            .order_by(Products.ProductName)
            .distinct(Products.__table__.c)
            )

    # results = query.all()
    # for row in query:
    #     print(row)

    session.close()
    return query



def sql5():
    """
    SELECT 
        ProductID, ProductName
    FROM
        products
    WHERE
        Discontinued = 'N'
    ORDER BY ProductName;
    """

    session = DbSession()  # 打开查询窗口
    query = (session.query(Products.ProductID, Products.ProductName).
                            filter(Products.Discontinued=='N').
                                order_by(Products.ProductName))
    # for row in query:
    #     print(row)

    session.close()

    return query


# ---------------------------------------------------------------------

def sql6():
    '''
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
    '''
    session = DbSession()  # 打开查询窗口
    # build the query
    query = (session.query(distinct(Order_Details.OrderID),
                    Order_Details.ProductID,
                    Products.ProductName,
                    Order_Details.UnitPrice,
                    Order_Details.Quantity,
                    Order_Details.Discount,
                    func.round(Order_Details.UnitPrice * Order_Details.Quantity * (1 - Order_Details.Discount), 2).label('ExtendedPrice'))
            .join(Order_Details, Products.ProductID == Order_Details.ProductID)
            .order_by(Order_Details.OrderID))
    session.close()
    return query

def sql7():
    '''
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
    ORDER BY a.CategoryName , b.ProductName , ProductSales;
    '''
    session = DbSession()  # 打开查询窗口
    # 注意3个join是倒叙的
    query = (session.query(
        Categories.CategoryID,
        Categories.CategoryName,
        Products.ProductName,
        func.sum(func.round(Order_Details.UnitPrice * Order_Details.Quantity * (1 - Order_Details.Discount), 2)).label("ProductSales")).
        join(Products, Categories.CategoryID == Products.CategoryID).
        join(Order_Details, Products.ProductID == Order_Details.ProductID).
        join(Orders, Orders.OrderID == Order_Details.OrderID).
        filter(Orders.OrderDate.between('1997/1/1', '1997/12/31')).
        group_by(Categories.CategoryID, Categories.CategoryName, Products.ProductName).
        order_by(Categories.CategoryName, Products.ProductName, "ProductSales"))

    # for row in query:
    #     print(row)
    session.close()
    return query

def sql8():
    '''
    SELECT DISTINCT
        ProductName AS Ten_Most_Expensive_Products, 
        UnitPrice
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
    '''

    session = DbSession()  # 打开查询窗口

    Products_aliased = aliased(Products)
    subquery = session.query(
            func.count(distinct(Products.UnitPrice))
        ).filter(Products.UnitPrice >= Products_aliased.UnitPrice).as_scalar()
    
    query = (
        session.query(
            Products.ProductName.label('Ten_Most_Expensive_Products'),
            Products.UnitPrice
        )
        .filter(10 >= subquery)
        .order_by(desc(Products.UnitPrice))
    )

    session.close()
    return query

def sql9():
    '''
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
    '''
    session = DbSession()  # 打开查询窗口

    query = (session.query(
            Categories.CategoryName, 
            Products.ProductName,
            Products.QuantityPerUnit,
            Products.UnitsInStock,
            Products.Discontinued).join(Products, Categories.CategoryID==Products.CategoryID).
                        filter(Products.Discontinued=='N').
                        order_by(Categories.CategoryName, Products.ProductName))
    # for row in query:
    #     print(row)
    session.close()
    return query

def sql10():
    '''
    SELECT 
        City, CompanyName, ContactName, 'Customers' AS Relationship
    FROM
        Customers 
    UNION 
    SELECT 
        City, CompanyName, ContactName, 'Suppliers'
    FROM
        Suppliers
    ORDER BY City , CompanyName;
    '''
    session = DbSession()  # 打开查询窗口

    table1 = session.query(
            Customers.City.label('City') , # new alias for union
            Customers.CompanyName.label('CompanyName'), # new alias for union
            Customers.ContactName,
            literal("'Customers'").label("Relationship")
        )
    table2 = session.query(
        Suppliers.City,
        Suppliers.CompanyName,
        Suppliers.ContactName,
        literal("'Suppliers'").label("Suppliers")
    )
    
    # union and sort - order by CANNOT access original columns! Either col's index or new alias
    query = table1.union(table2).order_by(
            'City', 'CompanyName'
        )
    
    # for row in query:
    #     print(row)
    session.close()
    return query
# ---------------------------------------------------------------------

def sql11():
    '''
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
    '''
    session = DbSession()  # 打开查询窗口

    subquery = session.query(Products).with_entities(func.avg(Products.UnitPrice)).subquery()
    query = session.query(Products.ProductName, Products.UnitPrice) \
        .filter(Products.UnitPrice > subquery) \
        .order_by(Products.UnitPrice) \
        .distinct()
    
    # for data in query:
    #     print(data.ProductName, data.UnitPrice)
    session.close()
    return query

def sql12():
    session = DbSession()  # 打开查询窗口
    # 定义子查询
    subquery = (
        session.query(
            Orders.ShippedDate,
            func.quarter(Orders.ShippedDate).label('ShippedQuarter'),
            Order_Details.ProductID,
            Order_Details.UnitPrice,
            Order_Details.Quantity,
            Order_Details.Discount,
        )
        .join(Order_Details)
        .join(Products)
        .join(Categories)
        .filter(
            Orders.ShippedDate.between('1997-01-01', '1997-12-31')
        )
        .subquery()
    )

    # 使用子查询
    query = (
        session.query(
            Categories.CategoryName,
            Products.ProductName,
            func.format(
                func.sum(
                    subquery.c.UnitPrice * subquery.c.Quantity * (1 - subquery.c.Discount)
                ),
                '.2f'
            ).label('ProductSales'),
            func.concat('Qtr ', subquery.c.ShippedQuarter).label('ShippedQuarter')
        )
        .join(Products, Categories.CategoryID == Products.CategoryID)
        .join(
            subquery,
            Products.ProductID == subquery.c.ProductID
        )
        .group_by(
            Categories.CategoryName,
            Products.ProductName,
            subquery.c.ShippedQuarter
        )
        .order_by(
            Categories.CategoryName,
            Products.ProductName,
            subquery.c.ShippedQuarter
        )
    )
    # result = query.all()
    # for data in result:
    #     print(data.CategoryName, data.ProductName, data.ProductSales, data.ShippedQuarter)
    session.close()
    return query

def sql13():
    '''
    select CategoryName, format(sum(ProductSales), 2) as CategorySales
    from
    (
        select distinct a.CategoryName,
            b.ProductName,
            format(sum(c.UnitPrice * c.Quantity * (1 - c.Discount)), 2) as ProductSales,
            concat('Qtr ', quarter(d.ShippedDate)) as ShippedQuarter
        from Categories as a
        inner join Products as b on a.CategoryID = b.CategoryID
        inner join Order_Details as c on b.ProductID = c.ProductID
        inner join Orders as d on d.OrderID = c.OrderID
        where d.ShippedDate between date('1997-01-01') and date('1997-12-31')
        group by a.CategoryName,
            b.ProductName,
            concat('Qtr ', quarter(d.ShippedDate))
        order by a.CategoryName,
            b.ProductName,
            ShippedQuarter
    ) as x
    group by CategoryName
    order by CategoryName;
    '''

    session = DbSession()  # 打开查询窗口
    # 声明表的别名
    Category = aliased(Categories)
    Product = aliased(Products)
    OrderDetail = aliased(Order_Details)
    Order = aliased(Orders)

    # 构建子查询
    subquery = (
        session.query(
            Category.CategoryName,
            Product.ProductName,
            func.format(
                func.sum(OrderDetail.UnitPrice * OrderDetail.Quantity * (1 - OrderDetail.Discount)), 2
            ).label('ProductSales'),
            func.concat('Qtr ', func.quarter(Order.ShippedDate)).label('ShippedQuarter')
        )
        .join(Product, Category.CategoryID == Product.CategoryID)
        .join(OrderDetail, Product.ProductID == OrderDetail.ProductID)
        .join(Order, Order.OrderID == OrderDetail.OrderID)
        .filter(Order.ShippedDate.between('1997-01-01', '1997-12-31'))
        .group_by(Category.CategoryName, Product.ProductName, 'ShippedQuarter')
        .order_by(Category.CategoryName, Product.ProductName, 'ShippedQuarter')
        .subquery()
    )

    # 构建主查询
    query = (
        session.query(
            subquery.c.CategoryName,
            func.format(func.sum(subquery.c.ProductSales), 2).label('CategorySales')
        )
        .group_by(subquery.c.CategoryName)
        .order_by(subquery.c.CategoryName)
    )

    # 执行查询
    # results = query.all()
    # for data in results:
    #     print([col for col in data])
    session.close()
    return query

def sql15():
    '''
    select distinct b.ShipName,
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
        concat(d.FirstName,  ' ', d.LastName) as Salesperson,
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
        e.UnitPrice * e.Quantity * (1 - e.Discount) as ExtendedPrice,
        b.Freight
    from Shippers a
    inner join Orders b on a.ShipperID = b.ShipVia
    inner join Customers c on c.CustomerID = b.CustomerID
    inner join Employees d on d.EmployeeID = b.EmployeeID
    inner join Order_Details e on b.OrderID = e.OrderID
    inner join Products f on f.ProductID = e.ProductID
    order by b.ShipName;
    '''

    session = DbSession()  # 打开查询窗口
    query = session.query(
        Orders.ShipName,
        Orders.ShipAddress,
        Orders.ShipCity,
        Orders.ShipRegion,
        Orders.ShipPostalCode,
        Orders.ShipCountry,
        Orders.CustomerID,
        Customers.CompanyName,
        Customers.Address,
        Customers.City,
        Customers.Region,
        Customers.PostalCode,
        Customers.Country,
        func.concat(Employees.FirstName, ' ', Employees.LastName).label('Salesperson'),
        Orders.OrderID,
        Orders.OrderDate,
        Orders.RequiredDate,
        Orders.ShippedDate,
        Shippers.CompanyName,
        Order_Details.ProductID,
        Products.ProductName,
        Order_Details.UnitPrice,
        Order_Details.Quantity,
        Order_Details.Discount,
        Order_Details.UnitPrice * Order_Details.Quantity * (1 - Order_Details.Discount).label(
            'ExtendedPrice'),
        Orders.Freight
    ). \
        join(Shippers, Shippers.ShipperID == Orders.ShipVia). \
        join(Customers, Customers.CustomerID == Orders.CustomerID). \
        join(Employees, Employees.EmployeeID == Orders.EmployeeID). \
        join(Order_Details, Orders.OrderID == Order_Details.OrderID). \
        join(Products, Products.ProductID == Order_Details.ProductID). \
        order_by(Orders.ShipName)

    # for data in query:
    #     print([col for col in data])
    session.close()
    return query

def sql16():
    '''
    select c.CategoryName as "Product Category",
           case when s.Country in
                     ('UK','Spain','Sweden','Germany','Norway',
                      'Denmark','Netherlands','Finland','Italy','France')
                then 'Europe'
                when s.Country in ('USA','Canada','Brazil')
                then 'America'
                else 'Asia-Pacific'
            end as "Supplier Continent",
            sum(p.UnitsInStock) as UnitsInStock
    from Suppliers s
    inner join Products p on p.SupplierID=s.SupplierID
    inner join Categories c on c.CategoryID=p.CategoryID
    group by c.CategoryName,
             case when s.Country in
                     ('UK','Spain','Sweden','Germany','Norway',
                      'Denmark','Netherlands','Finland','Italy','France')
                  then 'Europe'
                  when s.Country in ('USA','Canada','Brazil')
                  then 'America'
                  else 'Asia-Pacific'
             end;
    '''
    session = DbSession()  # 打开查询窗口

    s1 = aliased(Suppliers)
    c1 = aliased(Categories)

    # 构建查询语句
    stmt = (
        session.query(
            c1.CategoryName.label("Product Category"),
            case(
                    (s1.Country.in_(
                        ["UK", "Spain", "Sweden", "Germany", "Norway", "Denmark", "Netherlands", "Finland", "Italy",
                         "France"]), "Europe"),
                    (s1.Country.in_(["USA", "Canada", "Brazil"]), "America")
                ,
                else_="Asia-Pacific",
            ).label("Supplier Continent"),
            func.sum(Products.UnitsInStock).label("UnitsInStock")
        )
        .join(s1, Products.SupplierID == s1.SupplierID)
        .join(c1, Products.CategoryID == c1.CategoryID)
        .group_by(c1.CategoryName, case(
                (s1.Country.in_(
                    ["UK", "Spain", "Sweden", "Germany", "Norway", "Denmark", "Netherlands", "Finland", "Italy",
                     "France"]), "Europe"),
                (s1.Country.in_(["USA", "Canada", "Brazil"]), "America"),
            else_="Asia-Pacific",
        )
                  )
    )
    # for data in stmt:
    #     print([col for col in data])
    session.close()
    return stmt



if __name__ == "__main__":
    show_tables()
    # sql1()
    # sql10()
    # sql13()
    # sql15()
    # sql16()

