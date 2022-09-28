from mysql.connector import connect

DATABASE_NAME = "customerinfo"


def main():
    """Method for generating 15k document for mysql"""
    database = connect(host="127.0.0.1", port=3306, user="root", password="changeme")
    cursor = database.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS {DATABASE_NAME}")
    cursor.execute(f"CREATE DATABASE {DATABASE_NAME}")
    cursor.execute(f"USE {DATABASE_NAME}")
    for table in range(15):
        print(f"Adding data in {table}...")
        sql_query = f"CREATE TABLE IF NOT EXISTS customers_{table} (name VARCHAR(255), age int, PRIMARY KEY (name))"
        cursor.execute(sql_query)
        raws = []
        for raw_id in range(1000):
            raws.append((f"user_{raw_id}", raw_id))
        sql_query = f"INSERT INTO customers_{table}" + "(name, age) VALUES (%s, %s)"
        cursor.executemany(sql_query, raws)
    database.commit()


if __name__ == "__main__":
    main()
