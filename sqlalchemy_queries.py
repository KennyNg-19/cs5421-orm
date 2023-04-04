from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, inspect, MetaData, Table, func, distinct, desc, literal
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql import case
from memory_profiler import profile


ENABLE_PROFILING = False # 可以根据需要在这里切换memory profiler

# 创建对象的基类:
BaseModel = declarative_base()
database = 'northwind2'
user = 'root'
psw = '3.926puanY'
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


def show_tables():
    insp = inspect(engine)
    out = insp.get_table_names(schema='northwind2')
    print(out)




def controlled_profile(enabled=True, *args, **kwargs):
    def decorator(func):
        return profile(*args, **kwargs)(func) if enabled else func
    return decorator

# ---------------------------------------------------------------------

# Stream Profiling Results to Log File
fp = open("sqlalch_memory.log", "w+")

# 通过包装器控制 profile 的启用或禁用
controled_profile = controlled_profile(enabled=ENABLE_PROFILING, precision=4, stream=fp)


@controled_profile
def sql1():
    session = DbSession()  # 打开查询窗口
    subtotal = func.format(func.sum(Order_Details.UnitPrice * Order_Details.Quantity * (1 - Order_Details.Discount)),
                           2).label('Subtotal')
    query = session.query(Order_Details.OrderID, subtotal).group_by(Order_Details.OrderID).order_by(
        Order_Details.OrderID)


    # print(query)
    results = query.all()  # 执行查询（如果不调用，是不会真正去查询）
    # print(len(results))
    for row in results:
        continue
        # print(row)
    session.close()
    return query


@controled_profile
def sql2():
    session = DbSession()  # 打开查询窗口
    # 查询语句
    subtotal = func.sum(Order_Details.UnitPrice * Order_Details.Quantity * (1 - Order_Details.Discount)).label(
        'Subtotal')
    sub_query = session.query(Order_Details.OrderID, subtotal).group_by(Order_Details.OrderID).subquery()

    # query = (session.query(func.distinct(func.date(Orders.ShippedDate)).label('ShippedDate'),
    query = (session.query(func.date(Orders.ShippedDate).label('ShippedDate'),
                           Orders.OrderID,
                           sub_query.c.Subtotal,
                           func.year(Orders.ShippedDate).label('Year'))
             .join(sub_query, Orders.OrderID == sub_query.c.OrderID)
             .filter(Orders.ShippedDate.isnot(None), Orders.ShippedDate.between('1996-12-24', '1997-09-30'))
             .order_by("ShippedDate")  # 光执行SQL本身 不ORM：在select之后了，所以必须 by alias ONLY, not by Orders.ShippedDate
             # .order_by(Orders.ShippedDate)
             )
    # print(query)
    results = query.all()
    # print(len(results))
    for row in results:
        continue
        # print(row)
    session.close()
    return query


# def sql3():
#     pass
@controled_profile
def sql4():
    session = DbSession()  # 打开查询窗口

    query = (session.query(Products.__table__.c, Categories.CategoryName)
             .join(Categories, Categories.CategoryID == Products.CategoryID)
             .filter(Products.Discontinued == 'N')
             .order_by(Products.ProductName)
             # .distinct(Products.__table__.c)
             )
    # print(query)
    results = query.all()
    # print(len(results))
    for row in results:
        continue
        # print(row)

    session.close()
    return query


@controled_profile
def sql5():
    session = DbSession()  # 打开查询窗口
    query = (session.query(Products.ProductID, Products.ProductName).
             filter(Products.Discontinued == 'N').
             order_by(Products.ProductName))
    results = query.all()  # 执行查询（如果不调用，是不会真正去查询）
    # print(len(results))
    for row in results:
        continue
        # print(row)
    session.close()

    return query


# ---------------------------------------------------------------------
@controled_profile
def sql6():
    session = DbSession()  # 打开查询窗口
    # build the query
    # query = (session.query(distinct(Order_Details.OrderID),
    query = (session.query(Order_Details.OrderID,
                           Order_Details.ProductID,
                           Products.ProductName,
                           Order_Details.UnitPrice,
                           Order_Details.Quantity,
                           Order_Details.Discount,
                           func.round(Order_Details.UnitPrice * Order_Details.Quantity * (1 - Order_Details.Discount),
                                      2).label('ExtendedPrice'))
             .join(Order_Details, Products.ProductID == Order_Details.ProductID)
             .order_by(Order_Details.OrderID))
    # print(query)
    results = query.all()  # 执行查询（如果不调用，是不会真正去查询）
    # print(len(results))
    for row in results:
        continue
        # print(row)
    session.close()
    return query


@controled_profile
def sql7():
    session = DbSession()  # 打开查询窗口
    # 注意3个join是倒叙的
    query = (session.query(
        Categories.CategoryID,
        Categories.CategoryName,
        Products.ProductName,
        func.sum(func.round(Order_Details.UnitPrice * Order_Details.Quantity * (1 - Order_Details.Discount), 2)).label(
            "ProductSales")).
             join(Products, Categories.CategoryID == Products.CategoryID).
             join(Order_Details, Products.ProductID == Order_Details.ProductID).
             join(Orders, Orders.OrderID == Order_Details.OrderID).
             filter(Orders.OrderDate.between('1997/1/1', '1997/12/31')).
             group_by(Categories.CategoryID, Categories.CategoryName, Products.ProductName).
             order_by(Categories.CategoryName, Products.ProductName, "ProductSales"))
    # print(query)
    results = query.all()  # 执行查询（如果不调用，是不会真正去查询）
    # print(len(results))
    for row in results:
        continue
        # print(row)
    session.close()
    return query

@controled_profile
def sql8():
    session = DbSession()  # 打开查询窗口

    Products_aliased = aliased(Products)
    # subquery = session.query(func.count(distinct(Products.UnitPrice))).filter(Products.UnitPrice >= Products_aliased.UnitPrice).as_scalar()

    query = (
        session.query(
            distinct(Products.ProductName.label('Ten_Most_Expensive_Products')),
            Products.UnitPrice
        )
        .order_by(desc(Products.UnitPrice))
        .limit(10)
    )

    results = query.all()  # 执行查询（如果不调用，是不会真正去查询）
    session.close()
    return query


@controled_profile
def sql8_subquery():
    session = DbSession()  # 打开查询窗口

    Products_aliased = aliased(Products)
    subquery = session.query(func.count(distinct(Products.UnitPrice))).filter(
        Products.UnitPrice >= Products_aliased.UnitPrice).as_scalar()

    query = (
        session.query(
            Products.ProductName.label('Ten_Most_Expensive_Products'),
            Products.UnitPrice
        )
        .filter(10 >= subquery)
        .order_by(desc(Products.UnitPrice))
    )
    # print(query)
    results = query.all()  # 执行查询（如果不调用，是不会真正去查询）
    # print(len(results))
    for row in results:
        continue
        # print(row)
    session.close()
    return query


@controled_profile
def sql9():
    session = DbSession()  # 打开查询窗口

    query = (session.query(
        Categories.CategoryName,
        Products.ProductName,
        Products.QuantityPerUnit,
        Products.UnitsInStock,
        Products.Discontinued).join(Products, Categories.CategoryID == Products.CategoryID).filter(
        Products.Discontinued == 'N').
             order_by(Categories.CategoryName, Products.ProductName))
    # print(query)

    results = query.all()  # 执行查询（如果不调用，是不会真正去查询）
    # print(len(results))
    for row in results:
        continue
        # print(row)
    session.close()
    return query


@controled_profile
def sql10():
    session = DbSession()  # 打开查询窗口

    table1 = session.query(
        Customers.City.label('City'),  # new alias for union
        Customers.CompanyName.label('CompanyName'),  # new alias for union
        Customers.ContactName,
        literal("'Customers'").label("Relationship")
    )
    table2 = session.query(
        Suppliers.City,
        Suppliers.CompanyName,
        Suppliers.ContactName,
        literal("'Suppliers'").label("Relationship")
    )

    # union and sort - order by CANNOT access original columns! Either col's index or new alias
    query = table1.union(table2).order_by(
        'City', 'CompanyName'
    )
    # print(query)
    results = query.all()  # 执行查询（如果不调用，是不会真正去查询）
    # print(len(results))
    for row in results:
        continue
        # print(row)
    session.close()
    return query


# ---------------------------------------------------------------------
@controled_profile
def sql11():
    session = DbSession()  # 打开查询窗口

    subquery = session.query(Products).with_entities(func.avg(Products.UnitPrice)).subquery()
    query = session.query(Products.ProductName, Products.UnitPrice) \
        .filter(Products.UnitPrice > subquery) \
        .order_by(Products.UnitPrice) \
        .distinct()

    # print(query)
    results = query.all()  # 执行查询（如果不调用，是不会真正去查询）
    # print(len(results))
    for row in results:
        continue
        # print(row)
    session.close()
    return query


@controled_profile
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
    print(query)
    result = query.all()
    # for data in result:
    #     print(data.CategoryName, data.ProductName, data.ProductSales, data.ShippedQuarter)
    session.close()
    return query


@controled_profile
def sql13():
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

    print(query)
    # 执行查询
    results = query.all()
    # for data in results:
    #     print([col for col in data])
    session.close()
    return query


@controled_profile
def sql15():
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

    print(query)
    results = query.all()  # 执行查询（如果不调用，是不会真正去查询）
    # for data in query:
    #     print([col for col in data])
    session.close()
    return query


@controled_profile
def sql16():
    session = DbSession()  # 打开查询窗口

    s1 = aliased(Suppliers)
    c1 = aliased(Categories)

    # 构建查询语句
    query = (
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

    print(query)
    results = query.all()  # 执行查询（如果不调用，是不会真正去查询）
    # for data in stmt:
    #     print([col for col in data])
    session.close()
    return query


if __name__ == "__main__":
    # show_tables()
    # sql1()
    # sql2()
    sql7()
    # sql13()
    # sql15()
    # sql16()

