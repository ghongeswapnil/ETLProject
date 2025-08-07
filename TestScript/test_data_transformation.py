import pandas as pd
from sqlalchemy import create_engine
import pytest

import cx_Oracle
import os
os.environ['PATH'] ='C:\instantclient_23_5'

# Pre requisite
mysql_conn = create_engine("mysql+pymysql://root:root@localhost:3306/etldemo")

def test_DT_employee_first_name_derivation_check():
    query_expected = """select eno,substr(ename,1,instr(ename," ")-1) as first_name from staging_employees"""
    df_expected= pd.read_sql(query_expected, mysql_conn)
    query_actual = """select eno,first_name from employees_details"""
    df_actual = pd.read_sql(query_actual, mysql_conn)
    assert df_actual.equals(df_expected), "employees fisrt_name  did not converted  correctly - pls check"

def test_DT_employee_last_name_derivation_check():
    query_expected = """select eno,substr(ename,instr(ename," ")+1) as last_name from staging_employees"""
    df_expected= pd.read_sql(query_expected, mysql_conn)
    query_actual = """select eno,last_name from employees_details"""
    df_actual = pd.read_sql(query_actual, mysql_conn)
    assert df_actual.equals(df_expected), "employees last_name  did not converted  correctly - pls check"

def test_DT_employee_total_salary_derivation_check():
    query_expected = """select s.eno,s.salary+ifnull(s.commission,0) as total_salary from staging_salary as s"""
    df_expected= pd.read_sql(query_expected, mysql_conn)
    query_actual = """select eno,total_salary from employees_details"""
    df_actual = pd.read_sql(query_actual, mysql_conn)
    assert df_actual.equals(df_expected), "employees total_salary  did not converted  correctly - pls check"

def test_DT_emp_sal_joiner_check():
    query_expected = """select e.eno,substr(e.ename,1,instr(e.ename," ")-1) as first_name,substr(e.ename,instr(e.ename," ")+1) as last_name,
                e.hiredate,s.salary,ifnull(s.commission,0) as commission,s.salary+ifnull(s.commission,0) as total_salary 
                from staging_employees as e inner join staging_salary as s on e.eno = s.eno"""
    df_expected= pd.read_sql(query_expected, mysql_conn)
    query_actual = """select * from employees_details"""
    df_actual = pd.read_sql(query_actual, mysql_conn)
    assert df_actual.equals(df_expected), "Joiner  condition not working correctly - pls check"
