import pandas as pd
from sqlalchemy import create_engine

import cx_Oracle
import os
os.environ['PATH'] ='C:\instantclient_23_5'

# Pre requisite
oracle_conn = cx_Oracle.connect("system", "system", "localhost/xe")
# oracle_conn = create_engine("oracle+cx_oracle://system:system@localhost:1521/xe")   ----not working
mysql_conn = create_engine("mysql+pymysql://root:root@localhost:3306/etldemo")

print("ETL Process started")
 # Extraction

# 1. read salary data and write in to mysql staging_salary table.
df_salary = pd.read_json("E:\Pandas\ETLProject\Sources\salary.json")
df_salary.to_sql("staging_salary",mysql_conn,if_exists='replace',index=False)

# 2. read employees data from oracle and write in to mysql staging_employees table.
query = "select * from employees"
df_employees = pd.read_sql(query,oracle_conn)
df_employees.to_sql("staging_employees",mysql_conn,if_exists='replace',index=False)


# Transformation
query = """select e.eno,substr(e.ename,1,instr(e.ename," ")-1)as first_name,substr(e.ename,instr(e.ename," ")+1)as last_name,
        e.hiredate,s.salary,ifnull(s.commission,0)as commission,s.salary+ifnull(s.commission,0) as total_salary
        from staging_employees as e  inner join staging_salary as s on e.eno = s.eno"""

df_target = pd.read_sql(query,mysql_conn)


# Loading
df_target.to_sql("employees_details",mysql_conn,if_exists='replace',index=False)

print("ETL Process completed")