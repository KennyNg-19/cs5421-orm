from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, inspect, MetaData, Table, func, distinct
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql import case

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
    subquery = session.query(Products).with_entities(func.avg(Products.UnitPrice)).subquery()
    query = session.query(Products.ProductName, Products.UnitPrice) \
        .filter(Products.UnitPrice > subquery) \
        .order_by(Products.UnitPrice) \
        .distinct()
    for data in query:
        print(data.ProductName, data.UnitPrice)

def sql12():
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
    result = query.all()
    for data in result:
        print(data.CategoryName, data.ProductName, data.ProductSales, data.ShippedQuarter)

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
    results = query.all()
    for data in results:
        print([col for col in data])

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
    # 别名
    Order_Details_alias = aliased(Order_Details)

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
        func.round(Order_Details.UnitPrice * Order_Details.Quantity * (1 - Order_Details.Discount), 2).label(
            'ExtendedPrice'),
        Orders.Freight
    ). \
        join(Shippers, Shippers.ShipperID == Orders.ShipVia). \
        join(Customers, Customers.CustomerID == Orders.CustomerID). \
        join(Employees, Employees.EmployeeID == Orders.EmployeeID). \
        join(Order_Details_alias, Orders.OrderID == Order_Details_alias.OrderID). \
        join(Products, Products.ProductID == Order_Details_alias.ProductID). \
        distinct(Order_Details.ProductID). \
        order_by(Orders.ShipName)
    for data in query:
        print([col for col in data])

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
                (s1.Country.in_(["USA", "Canada", "Brazil"]), "America")
            ,
            else_="Asia-Pacific",
        )
                  )
    )
    for data in stmt:
        print([col for col in data])


if __name__ == "__main__":
    show_tables()
    sql11()
    sql12()
    sql13()
    # sql15() # 耗时太长
    sql16()
