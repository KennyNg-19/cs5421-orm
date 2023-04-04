############################################################################
## Django ORM Standalone Python Template
############################################################################
""" Here we'll import the parts of Django we need. It's recommended to leave
these settings as is, and skip to START OF APPLICATION section below """

# Turn off bytecode generation
import sys

from memory_profiler import profile
from django.db.models import *
from django.db.models.functions import ExtractYear, TruncDate
# from app.models import *

sys.dont_write_bytecode = True

def controlled_profile(enabled=True, *args, **kwargs):
    def decorator(func):
        return profile(*args, **kwargs)(func) if enabled else func
    return decorator

# ---------------------------------------------------------------------

# Stream Profiling Results to Log File
fp = open("sqlalch_memory.log", "w+")

# 通过包装器控制 profile 的启用或禁用
ENABLE_PROFILING = False # 可以根据需要在这里切换
controled_profile = controlled_profile(enabled=ENABLE_PROFILING, precision=4, stream=fp)

# Django specific settings
import os

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
import django

django.setup()

# Import your models for use in your script
from app.models import *
from django.db.models import Sum, F, Q
import django.db.models as models


############################################################################
################# START OF APPLICATION
@controled_profile
def sql_function_1():
    # 1
    """
        select OrderID, format(sum(UnitPrice * Quantity * (1 - Discount)), 2) as Subtotal
        from orderdetails
        group by OrderID
        order by OrderID;
        """
    query = (
        OrderDetails.objects
        .values('orderid')
        .annotate(
            Subtotal=Sum(F('unitprice') * F('quantity') * (1 - F('discount')), output_field=models.FloatField())
        )
        .order_by('orderid')
    )
    # print(subtotal_by_order.query)
    # print(subtotal_by_order)
    # length = len(subtotal_by_order)
    # print(len(query))
    for data in query:
        continue
        # print(data)
    return len(query)


@controled_profile
def sql_function_2():
    # 2
    """
    select distinct date(a.ShippedDate) as ShippedDate,
                a.OrderID,
                b.Subtotal,
                year(a.ShippedDate) as Year
    from Orders a
             inner join
         (
             select distinct OrderID,
                             format(sum(UnitPrice * Quantity * (1 - Discount)), 2) as Subtotal
             from orderdetails
             group by OrderID) b on a.OrderID = b.OrderID
    where a.ShippedDate is not null
      and a.ShippedDate between date('1996-12-24') and date('1997-09-30')
    order by a.ShippedDate;
    :return:
    """
    query = OrderDetails.objects.select_related('OrderID_id').filter(
        orderid_id__shippeddate__isnull=False,
        orderid_id__shippeddate__range=['1996-12-24', '1997-09-30']
    ) \
        .annotate(year=ExtractYear('orderid_id__shippeddate')) \
        .annotate(ShippedDate=TruncDate('orderid_id__shippeddate')) \
        .annotate(
        Subtotal=Sum(F('unitprice') * F('quantity') * (1 - F('discount')),
                     output_field=models.FloatField())). \
        values('orderid_id__shippeddate', 'orderid_id__orderid', 'Subtotal', 'year'). \
        order_by('ShippedDate')
    # print(orders.query)
    # print(len(orders))
    # print(len(query))
    for data in query:
        continue
        # print(data)
    return len(query)


@controled_profile
def sql_function_3():
    # 3
    """
    select distinct b.*, a.CategoryName
    from Categories a
    inner join Products b on a.CategoryID = b.CategoryID
    where b.Discontinued = 'N'
    order by b.ProductName;
    :return:
    """
    query = (
        Categories.objects.filter(
            products__discontinued=1
        )
        .annotate(
            CategoryName=F('categoryname'),
        )
        .values(
            'products__productid',
            'products__productname',
            'products__supplierid',
            'products__categoryid',
            'products__quantityperunit',
            'products__unitsinstock',
            'products__unitsonorder',
            'products__reorderlevel',
            'products__discontinued',
            'CategoryName'
        )
        .order_by('products__productname')
    )

    # print(categories.query)
    # print(categories)

    # print(len(query))
    for data in query:
        continue
        # print(data)
    return len(query)


@controled_profile
def sql_function_4():
    # 4
    """
    select distinct b.*, a.CategoryName
    from Categories a
    inner join Products b on a.CategoryID = b.CategoryID
    where b.Discontinued = 1
    order by b.ProductName;
    :return:
    """

    query = Products.objects.filter(discontinued='N') \
        .select_related('categoryid').annotate().order_by('productname') \
        .values()
        # .values('productid', 'productname', 'categoryid__categoryid', 'categoryid__categoryname')
    # print(categories.query)
    # print(categories)

    # print(len(query))
    for data in query:
        continue
        # print(data)
    return len(query)


@controled_profile
def sql_function_5():
    # 5
    """
    select ProductID, ProductName
    from products
    where Discontinued = 1
    order by ProductName;
    :return:
    """
    query = (
        Products.objects.filter(
            discontinued='N'
        )
        .values(
            'productid',
            'productname'
        )
        .order_by('productname')
    )
    # print(products.query)
    # print(len(products.values()))
    # print(len(query))
    for data in query:
        continue
        # print(data)
    return len(query)

@controled_profile
def sql_function_6():
    # 6
    query = OrderDetails.objects.select_related('productid').annotate(
        ProductName=F('productid__productname'),
        ExtendedPrice=Sum(F('unitprice') * F('quantity') * (1 - F('discount')))
    ).values(
        'orderid', 'productid', 'ProductName', 'unitprice', 'quantity', 'discount', 'ExtendedPrice'
    ).order_by('orderid')
    # print(q.query)
    # print(len(q))
    # print(len(query))
    for data in query:
        continue
        # print(data)
    return len(query)

@controled_profile
def sql_function_8():
    # 8
    query = Products.objects.annotate(
        Ten_Most_Expensive_Products=F('productname'),
        UnitPrice=F('unitprice')
    ).order_by('-unitprice')
    # print(q.query)
    # print(len(q))
    # print(len(query))
    for data in query:
        continue
        # print(data)
    return len(query)

@controled_profile
def sql_function_9():
    # 9
    query = Products.objects.filter(
        discontinued='N'
    ).select_related('categoryid').order_by(
        'categoryid__categoryname', 'productname'
    ).values(
        'categoryid__categoryname', 'productname', 'quantityperunit',
        'unitsinstock', 'discontinued'
    ).distinct()
    # print(q.query)
    # print(len(q))
    # print(len(query))
    for data in query:
        continue
        # print(data)
    return len(query)

@controled_profile
def sql_function_10():
    # 10
    # Define queries for Customers and Suppliers separately
    customers_query = Customers.objects.annotate(Relationship=Value('Customers', output_field=CharField()))
    suppliers_query = Suppliers.objects.annotate(Relationship=Value('Suppliers', output_field=CharField()))
    # Combine queries using union operator
    union_query = customers_query.union(suppliers_query)
    # Specify columns to select and order by
    query = union_query.values('city', 'companyname', 'contactname', 'Relationship').order_by('city', 'companyname')
    # print(q.query)
    # print(len(q))
    # print(len(query))
    for data in query:
        continue
        # print(data)
    return len(query)

@controled_profile
def sql_function_11():
    # 11
    query = Products.objects.filter(
        unitprice__gt=Products.objects.aggregate(avg_price=Avg('unitprice'))['avg_price']).order_by('unitprice').values(
        'productname', 'unitprice').distinct()
    # print(q.query)
    # print(list(q))
    # print(len(query))
    for data in query:
        continue
        # print(data)
    return len(query)

def run():
    from datetime import datetime

    func_list = [orderSubTotals, salesByYear, employeeSalesByCountry, listOfProducts, currentProductList]
    for func in func_list:
        start_time = datetime.now()
        func()
        execute_time = datetime.now() - start_time
        print(func.__name__, "execute time: ", execute_time)


if __name__ == '__main__':
    run()