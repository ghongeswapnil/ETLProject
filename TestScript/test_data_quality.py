import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import pytest


def test_DQ_salary_data_NULL_value_check():
    df= pd.read_json("Sources/salary.json")
    null_counts = df.isnull().sum().sum()
    assert null_counts == 0,"There are nulls in my file - pls check"

def test_DQ_salary_data_duplicate_value_check():
    df= pd.read_json("Sources/salary.json")
    dupes_counts = df.duplicated().sum()
    assert dupes_counts == 0,"There are duplocates in my file - pls check"