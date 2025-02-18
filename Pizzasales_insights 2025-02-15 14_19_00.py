# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import *

# COMMAND ----------

df1=spark.read.load('/FileStore/tables/pizza_sales/orders-2.csv',format='csv',sep=',',header='true',escape='"',inferschema='true')

# COMMAND ----------

df2=spark.read.load('/FileStore/tables/pizza_sales/pizzas-2.csv',format='csv',sep=',',header='true',escape='"',inferschema='true')

# COMMAND ----------

df3=spark.read.load('/FileStore/tables/pizza_sales/order_details-2.csv',format='csv',sep=',',header='true',escape='"',inferschema='true')

# COMMAND ----------

df4=spark.read.load('/FileStore/tables/pizza_sales/pizza_types-2.csv',format='csv',sep=',',header='true',escape='"',inferschema='true')

# COMMAND ----------

df1.createOrReplaceTempView('orders')
df2.createOrReplaceTempView('pizzas')
df3.createOrReplaceTempView('order_details')
df4.createOrReplaceTempView('pizza_types')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from order_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pizzas

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pizza_types

# COMMAND ----------

# DBTITLE 1,Retrieve the total number of orders placed.
# MAGIC %sql
# MAGIC select count(order_id) from order_details 

# COMMAND ----------

# DBTITLE 1,Calculate the total revenue generated from pizza sales.
# MAGIC %sql
# MAGIC select sum(order_details.quantity*pizzas.price)total_revenue from order_details join pizzas on order_details.pizza_id=pizzas.pizza_id 

# COMMAND ----------

# DBTITLE 1,Identify the highest-priced pizza.
# MAGIC %sql
# MAGIC select pizza_types.name,pizzas.price from pizza_types join pizzas on pizza_types.pizza_type_id=pizzas.pizza_type_id order by pizzas.price desc limit 1

# COMMAND ----------

# DBTITLE 1,Identify the most common pizza size ordered.
# MAGIC %sql
# MAGIC select pizzas.size,count(order_details.order_details_id)order_count from order_details join pizzas on pizzas.pizza_id=order_details.pizza_id group by pizzas.size order by order_count desc

# COMMAND ----------

# DBTITLE 1,List the top 5 most ordered pizza types along with their quantities.
# MAGIC %sql
# MAGIC select  pizzas.pizza_type_id,sum(order_details.quantity) from order_details join pizzas on pizzas.pizza_id=order_details.pizza_id group by pizza_type_id,quantity order by sum(quantity) desc
