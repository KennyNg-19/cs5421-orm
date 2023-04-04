# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey and OneToOneField has `on_delete` set to the desired behavior
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models


class Categories(models.Model):
    categoryid = models.AutoField(db_column='CategoryID', primary_key=True)  # Field name made lowercase.
    categoryname = models.CharField(db_column='CategoryName', unique=True, max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    description = models.TextField(db_column='Description', db_collation='utf8_unicode_ci')  # Field name made lowercase.
    picture = models.CharField(db_column='Picture', max_length=50, db_collation='utf8_unicode_ci')  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'categories'


class Customers(models.Model):
    customerid = models.CharField(db_column='CustomerID', primary_key=True, max_length=5, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    companyname = models.CharField(db_column='CompanyName', max_length=40, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    contactname = models.CharField(db_column='ContactName', max_length=30, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    contacttitle = models.CharField(db_column='ContactTitle', max_length=30, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    address = models.CharField(db_column='Address', max_length=60, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    city = models.CharField(db_column='City', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    region = models.CharField(db_column='Region', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    postalcode = models.CharField(db_column='PostalCode', max_length=10, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    country = models.CharField(db_column='Country', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    phone = models.CharField(db_column='Phone', max_length=24, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    fax = models.CharField(db_column='Fax', max_length=24, db_collation='utf8_unicode_ci')  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'customers'


class Employees(models.Model):
    employeeid = models.AutoField(db_column='EmployeeID', primary_key=True)  # Field name made lowercase.
    lastname = models.CharField(db_column='LastName', max_length=20, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    firstname = models.CharField(db_column='FirstName', max_length=10, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    title = models.CharField(db_column='Title', max_length=30, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    titleofcourtesy = models.CharField(db_column='TitleOfCourtesy', max_length=25, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    birthdate = models.DateTimeField(db_column='BirthDate')  # Field name made lowercase.
    hiredate = models.DateTimeField(db_column='HireDate')  # Field name made lowercase.
    address = models.CharField(db_column='Address', max_length=60, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    city = models.CharField(db_column='City', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    region = models.CharField(db_column='Region', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    postalcode = models.CharField(db_column='PostalCode', max_length=10, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    country = models.CharField(db_column='Country', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    homephone = models.CharField(db_column='HomePhone', max_length=24, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    extension = models.CharField(db_column='Extension', max_length=4, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    photo = models.CharField(db_column='Photo', max_length=50, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    notes = models.TextField(db_column='Notes', db_collation='utf8_unicode_ci', blank=True, null=True)  # Field name made lowercase.
    reportsto = models.ForeignKey('self', models.DO_NOTHING, db_column='ReportsTo', blank=True, null=True)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'employees'


class OrderDetails(models.Model):
    id = models.AutoField(db_column='ID', primary_key=True)  # Field name made lowercase.
    orderid = models.ForeignKey('Orders', models.DO_NOTHING, db_column='OrderID')  # Field name made lowercase.
    productid = models.ForeignKey('Products', models.DO_NOTHING, db_column='ProductID')  # Field name made lowercase.
    unitprice = models.FloatField(db_column='UnitPrice')  # Field name made lowercase.
    quantity = models.PositiveSmallIntegerField(db_column='Quantity')  # Field name made lowercase.
    discount = models.FloatField(db_column='Discount')  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'order_details'
        unique_together = (('orderid', 'productid'),)


class Orders(models.Model):
    orderid = models.AutoField(db_column='OrderID', primary_key=True)  # Field name made lowercase.
    customerid = models.ForeignKey(Customers, models.DO_NOTHING, db_column='CustomerID')  # Field name made lowercase.
    employeeid = models.ForeignKey(Employees, models.DO_NOTHING, db_column='EmployeeID')  # Field name made lowercase.
    orderdate = models.DateTimeField(db_column='OrderDate')  # Field name made lowercase.
    requireddate = models.DateTimeField(db_column='RequiredDate', blank=True, null=True)  # Field name made lowercase.
    shippeddate = models.DateTimeField(db_column='ShippedDate', blank=True, null=True)  # Field name made lowercase.
    shipvia = models.ForeignKey('Shippers', models.DO_NOTHING, db_column='ShipVia')  # Field name made lowercase.
    freight = models.FloatField(db_column='Freight')  # Field name made lowercase.
    shipname = models.CharField(db_column='ShipName', max_length=40, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    shipaddress = models.CharField(db_column='ShipAddress', max_length=60, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    shipcity = models.CharField(db_column='ShipCity', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    shipregion = models.CharField(db_column='ShipRegion', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    shippostalcode = models.CharField(db_column='ShipPostalCode', max_length=10, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    shipcountry = models.CharField(db_column='ShipCountry', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'orders'


class Products(models.Model):
    productid = models.AutoField(db_column='ProductID', primary_key=True)  # Field name made lowercase.
    productname = models.CharField(db_column='ProductName', max_length=40, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    supplierid = models.ForeignKey('Suppliers', models.DO_NOTHING, db_column='SupplierID')  # Field name made lowercase.
    categoryid = models.ForeignKey(Categories, models.DO_NOTHING, db_column='CategoryID')  # Field name made lowercase.
    quantityperunit = models.CharField(db_column='QuantityPerUnit', max_length=20, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    unitprice = models.FloatField(db_column='UnitPrice')  # Field name made lowercase.
    unitsinstock = models.PositiveSmallIntegerField(db_column='UnitsInStock')  # Field name made lowercase.
    unitsonorder = models.PositiveSmallIntegerField(db_column='UnitsOnOrder')  # Field name made lowercase.
    reorderlevel = models.PositiveSmallIntegerField(db_column='ReorderLevel')  # Field name made lowercase.
    discontinued = models.CharField(db_column='Discontinued', max_length=1, db_collation='utf8_unicode_ci')  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'products'


class Shippers(models.Model):
    shipperid = models.AutoField(db_column='ShipperID', primary_key=True)  # Field name made lowercase.
    companyname = models.CharField(db_column='CompanyName', max_length=40, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    phone = models.CharField(db_column='Phone', max_length=24, db_collation='utf8_unicode_ci')  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'shippers'


class Suppliers(models.Model):
    supplierid = models.AutoField(db_column='SupplierID', primary_key=True)  # Field name made lowercase.
    companyname = models.CharField(db_column='CompanyName', max_length=40, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    contactname = models.CharField(db_column='ContactName', max_length=30, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    contacttitle = models.CharField(db_column='ContactTitle', max_length=30, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    address = models.CharField(db_column='Address', max_length=60, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    city = models.CharField(db_column='City', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    region = models.CharField(db_column='Region', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    postalcode = models.CharField(db_column='PostalCode', max_length=10, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    country = models.CharField(db_column='Country', max_length=15, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    phone = models.CharField(db_column='Phone', max_length=24, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    fax = models.CharField(db_column='Fax', max_length=24, db_collation='utf8_unicode_ci')  # Field name made lowercase.
    homepage = models.CharField(db_column='HomePage', max_length=255)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'suppliers'
