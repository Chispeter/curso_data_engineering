### This script moves data from sources (in this case, SQL Server) to Snowflake, i.e., data ingestion

# Import Pandas and Open DataBase Connectivity for Python
import pandas as pd
import pyodbc


def model(dbt, session):
    # dbt configuration
    dbt.config(packages=["pyodbc", "pandas"], tags="data_movement")

    
    # Return final dataframe
    return table_names_result
