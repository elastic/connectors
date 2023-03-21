#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import random
import string
import subprocess as sp

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
DATABASE_NAME = "xe"
_SIZES = {"small": 5, "medium": 10, "large": 30}
NUM_TABLES = _SIZES[DATA_SIZE]
CONNECTION_STRING = "DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=127.0.0.1,9090;UID={user};PWD=Password_123;TrustServerCertificate=yes"
DRIVER_PATH = "/opt/mssql-tools18/bin"
USER = "admin"


def random_text(k=1024 * 20):
    """Function to generate random text

    Args:
        k (int, optional): size of data in bytes. Defaults to 1024*20.

    Returns:
        string: random text
    """
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))


BIG_TEXT = random_text()


def inject_lines(table, cursor, start, lines):
    """Ingest rows in table

    Args:
        table (str): Name of table
        cursor (cursor): Cursor to execute query
        start (int): Starting row
        lines (int): Number of rows
    """
    rows = []
    for row_id in range(lines):
        row_id += start
        rows.append((f"user_{row_id}", row_id, BIG_TEXT))
    sql_query = (
        f"INSERT INTO customers_{table} (name, age, description) VALUES (?, ?, ?)"
    )
    cursor.executemany(sql_query, rows)


def append_to_path(current_path):
    """set the environment variable"""
    if DRIVER_PATH not in current_path:
        os.environ["PATH"] += os.pathsep + DRIVER_PATH


# Microsoft ODBC Driver: https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
def setup():
    """Install ODBC Driver"""
    system_name = sp.getoutput("uname").lower()
    if "linux" in system_name:
        os_name = sp.getoutput('hostnamectl | grep "Operating System"').lower()
        current_path = sp.getoutput("echo $PATH")

        # Install ODBC Driver for Ubuntu
        if "ubuntu" in os_name:
            os.system(
                "sudo su -c 'curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -; curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list'"
            )
            os.system("sudo apt-get update")
            os.system(
                "sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 unixodbc-dev"
            )
            append_to_path(current_path)

        # Install ODBC Driver for centos, rocky linux and alma linux
        else:
            for operating_system in ["centos", "rocky", "alma"]:
                if operating_system in os_name:
                    # Download for version 7
                    if "7" in os_name:
                        os.system(
                            "sudo curl https://packages.microsoft.com/config/rhel/7/prod.repo > /etc/yum.repos.d/mssql-release.repo"
                        )

                    # Download for version 8
                    else:
                        os.system(
                            "sudo curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo"
                        )
                    os.system("sudo yum remove unixODBC-utf16 unixODBC-utf16-devel")
                    os.system(
                        "sudo ACCEPT_EULA=Y yum install -y msodbcsql18 mssql-tools18 unixODBC-devel"
                    )
                    append_to_path(current_path)
                    break
    # Install ODBC Driver for darwin
    elif "darwin" in system_name:
        os.system(
            '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"'
        )
        os.system(
            "yes | brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release"
        )
        os.system("yes | brew update")
        os.system("yes | HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18")
    else:
        raise Exception("Unsupported Operating System")


def load():
    """N tables of 10001 rows each. each row is ~ 1024*20 bytes"""
    import pyodbc

    database_sa = pyodbc.connect(CONNECTION_STRING.format(user="sa"))
    database_sa.autocommit = True
    cursor = database_sa.cursor()
    cursor.execute("CREATE LOGIN admin WITH PASSWORD = 'Password_123'")
    cursor.execute("ALTER SERVER ROLE [sysadmin] ADD MEMBER [admin]")
    database_sa.autocommit = False
    cursor.close()
    database_sa.close()
    database = pyodbc.connect(CONNECTION_STRING.format(user=USER))
    database.autocommit = True
    cursor = database.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE_NAME}")
    cursor.execute(f"CREATE DATABASE {DATABASE_NAME}")
    cursor.execute(f"USE {DATABASE_NAME}")
    database.autocommit = False

    for table in range(NUM_TABLES):
        print(f"Adding data in {table}...")
        sql_query = f"CREATE TABLE customers_{table} (name VARCHAR(255), age int, description TEXT, PRIMARY KEY (name))"
        cursor.execute(sql_query)
        for i in range(10):
            inject_lines(table, cursor, i * 1000, 1000)
    database.commit()


def remove():
    """Removes 10 random items per table"""
    import pyodbc

    database = pyodbc.connect(CONNECTION_STRING.format(user=USER))
    cursor = database.cursor()
    cursor.execute(f"USE {DATABASE_NAME}")
    for table in range(NUM_TABLES):
        rows = [(f"user_{row_id}",) for row_id in random.sample(range(1, 1000), 10)]
        sql_query = f"DELETE from customers_{table} where name=?"
        cursor.executemany(sql_query, rows)
    database.commit()
