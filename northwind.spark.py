import findspark
findspark.init()
import pyspark # Call this only after findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

# to be used with pyspark
# tested to work with spark 2.0 and python 3.5

# create custom clear command to clear screen
import os, platform
system=platform.system()
if system.upper()=='WINDOWS':   
    clear = lambda: os.system('cls')
else:
    clear = lambda: os.system('clear')

# import all the csvfiles and create tables in spark
entities=['categories', 'employee_territories',  'products',  'suppliers', 'customers', 'order_details', 'regions', 'territories', 'employees',  'orders',  'shippers']
dataframes={}
#load all csv files as dataframes and create tables for them
for entity in entities:
    print("registering table", entity)
    dataframes[entity]=spark.read.csv(entity+'.csv', header=True,inferSchema=True)
    #dataframes[entity].cache()
    dataframes[entity].createOrReplaceTempView(entity)

for entity in entities:
    dataframes[entity].printSchema()

for entity in entities:
    dataframes[entity].cache()


customerDF =spark.read.csv('customers.csv',header=True,inferSchema=True)
#show the schema of this table
customerDF.printSchema()
customerDF.createOrReplaceTempView("customers")
df2=spark.sql("select * from customers limit 10")


#group by query example
df2=spark.sql("select min(OrderDate), max(orderdate) from orders")
df2.show()

from pyspark.sql import functions as F
# df.agg(F.min(df.OrderDate), F.max(df.OrderDate)).show() 

# #join examples
# # select top customers by city 
# spark.sql(" \
#     select  \
#         c.city, sum(od.UnitPrice* od.Quantity) as gms \
#     from \
#         orders o inner join customers c on o.CustomerId=c.CustomerId \
#         inner join order_details od on o.OrderId=Od.OrderId \
#     group by c.city order by gms " 
#         ).show()

# #equivalent python code
orders=dataframes['orders']
customers=dataframes['customers']
order_details=dataframes['order_details']
orders.join(customers,orders.CustomerID == customers.CustomerID) \
    .join(order_details, orders.OrderID == order_details.OrderID) \
    .groupBy(customers.City) \
    .agg(F.sum(order_details.UnitPrice * order_details.Quantity).alias('gms')) \
    .orderBy('gms') \
    .show()

# #sales by year
# # Sellers who have got sales on both january and february 1997
# # explain query example
# spark.sql('select s.companyName,count(distinct to_date(o.orderdate)) \
#     from orders o \
#         inner join order_details od \
#             on o.orderid=od.orderid \
#         inner join products p \
#             on od.productid= p.productid \
#         inner join suppliers s \
#             on p.supplierid =s.supplierid \
#         where \
#             o.orderdate between   to_date("1997-01-01") and to_date("1997-02-28") \
#         group by s.companyName ').explain(True)
        

# # execute query
# spark.sql('select s.companyName,count(distinct trunc(o.orderdate,"month")) as monthcount \
#     from orders o \
#         inner join order_details od \
#             on o.orderid=od.orderid \
#         inner join products p \
#             on od.productid= p.productid \
#         inner join suppliers s \
#             on p.supplierid =s.supplierid \
#         where \
#             o.orderdate between   to_date("1997-01-01") and to_date("1997-02-28") \
#         group by s.companyName \
#         having monthcount=2').show()

# ## sellers who have got orders in each of the first six months of 1997
# # spark sql code
# spark.sql('select s.companyName,count(distinct trunc(o.orderdate,"month")) as monthcount \
#     from orders o \
#         inner join order_details od \
#             on o.orderid=od.orderid \
#         inner join products p \
#             on od.productid= p.productid \
#         inner join suppliers s \
#             on p.supplierid =s.supplierid \
#         where \
#             o.orderdate between   to_date("1997-01-01") and to_date("1997-06-30") \
#         group by s.companyName \
#         having monthcount=6').show()
 
# #equivalent python code using dataframes
# orders=dataframes['orders']
# order_details=dataframes['order_details']
# products=dataframes['products']
# suppliers=dataframes['suppliers']

# joined=orders.filter(orders.OrderDate >= '1997-01-01').filter(orders.OrderDate <= '1997-06-30' )\
#     .join(order_details, orders.OrderID==order_details.OrderID)\
#     .join(products, order_details.ProductID==products.ProductID)\
#     .join(suppliers, products.SupplierID==suppliers.SupplierID)\
#     .groupBy(suppliers.CompanyName)\
#     .agg(F.countDistinct(F.trunc(orders.OrderDate,"month")).alias('monthcount'))
# joined.filter(joined.monthcount==6)\
#     .show()
