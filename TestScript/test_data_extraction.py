import pandas as pd
from sqlalchemy import create_engine
import pytest

import cx_Oracle
import os
os.environ['PATH'] ='C:\instantclient_23_5'

# Pre requisite
oracle_conn = cx_Oracle.connect("system", "system", "localhost/xe")
# oracle_conn = create_engine("oracle+cx_oracle://system:system@localhost:1521/xe")   ----not working
mysql_conn = create_engine("mysql+pymysql://root:root@localhost:3306/etldemo")

def test_DE_salary_data_to_staging():
    df_expected = pd.read_json("E:\Pandas\ETLProject\Sources\salary.json")
    df_actual = pd.read_sql("""select * from staging_salary""",mysql_conn)
    assert df_actual.equals(df_expected),"salary data did not extracted correctly - pls check"

def test_DE_employees_data_to_staging():
    df_expected = pd.read_sql("""select * from employees""",oracle_conn)
    df_actual = pd.read_sql("""select * from staging_employees""",mysql_conn)
    assert df_actual.equals(df_expected),"employees data did not extracted correctly - pls check"