### This script moves data from sources (in this case, SQL Server) to Snowflake, i.e., data ingestion

# Import Pandas and Open DataBase Connectivity for Python
import pandas as pd
import pyodbc


def model(dbt, session):
    # dbt configuration
    dbt.config(packages=["pyodbc", "pandas"], tags="data_movement")

    ## GETTING DATA FROM SQL SERVER
    # Create variables for SQL Server connection credentials 
    driver_name = 'SQL Server'
    server_name = 'CHISPETERPC\TEW_SQLEXPRESS'
    database_name = 'Civica'

    # Create a connection string variable using string interpolation
    connection_string = f"DRIVER={driver_name};SERVER={server_name};DATABASE={database_name};TRUSTED_CONNECTION=YES"

    # Create a connection SQL Database (SQL Server)
    conn_sql = pyodbc.connect(connection_string)
    cursor_sql = conn_sql.cursor()

    # Ejemplo tabla
    table_names_query = """
    IF EXISTS (SELECT * FROM """ + database_name + """.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'sysdiagrams')
    BEGIN
        SELECT TABLE_NAME FROM """ + database_name + """.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'base table' AND TABLE_NAME != 'sysdiagrams' ORDER BY TABLE_NAME ASC
    END
    ELSE
    BEGIN
        SELECT TABLE_NAME FROM """ + database_name + """.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'base table' ORDER BY TABLE_NAME ASC
    END;
    """
    table_names_result = pd.read_sql(table_names_query, conn_sql)
    
    #for table in table_names_result:


    # Utilizar merge etc...
    # Close the SQL server connection
    cursor_sql.close()
    conn_sql.close()


    ## MOVING DATA FROM SQL SERVER TO SNOWFLAKE
    session.execute("""
    CREATE TABLE ALUMNO23_DEV_BRONZE_DB.a.test(
        ORDER_ID VARCHAR(256),
        PRODUCT_ID VARCHAR(256),
        QUANTITY VARCHAR(256)
    );
    """)

    # Close the Snowflake connection
    session.close()

    # Return final dataframe
    return table_names_result
