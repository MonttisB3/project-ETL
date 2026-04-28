import mysql.connector
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_HOST, DB_USER, DB_PASSWORD

#Establishing connection to the local mysql server
try:
    mydb = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = mydb.cursor()
    print (f"\nDatabase connection to: {mydb}")
except mysql.connector.Error as e:
    print("\nProblem connecting to the databse:" ,e)
    exit()


#Creating the used databases for the project work
cursor.execute("CREATE DATABASE IF NOT EXISTS rawDB")
cursor.execute("CREATE DATABASE IF NOT EXISTS cleanDB")
mydb.commit()
print("Databases ensured")

#Creating the tables for rawDB database
cursor.execute("USE rawDB")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
        name VARCHAR(255),
        age INT,
        location VARCHAR(255)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
        name VARCHAR(255),
        category VARCHAR(255),
        price DECIMAL(10,2)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
        customer_id INT,
        product_id INT,
        quantity INT,
        payment_method varchar(255),
        order_date DATE,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    )
""")

mydb.commit()
print("rawDB tables ensured")

#Creating the tables for cleanDB database. Has unique constraint for checking duplicates later
cursor.execute("USE cleanDB")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers_clean (
        customer_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
        source VARCHAR(50) NOT NULL DEFAULT 'unknown',
        name VARCHAR(255),
        age INT,
        location VARCHAR(255),
        CONSTRAINT unique_customer UNIQUE (name, age, location)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS products_clean (
        product_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
        source VARCHAR(50) NOT NULL DEFAULT 'unknown',
        name VARCHAR(255),
        category VARCHAR(255),
        price DECIMAL(10,2),
        CONSTRAINT unique_product UNIQUE (name, price)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders_clean (
        order_id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
        source VARCHAR(50) NOT NULL DEFAULT 'unknown',
        customer_id INT,
        product_id INT,
        quantity INT,
        payment_method varchar(255),
        order_date DATE,
        FOREIGN KEY (customer_id) REFERENCES customers_clean(customer_id),
        FOREIGN KEY (product_id) REFERENCES products_clean(product_id),
        CONSTRAINT unique_order UNIQUE (customer_id, product_id, order_date)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS country_info (
        country_code VARCHAR(2) PRIMARY KEY,
        country_name VARCHAR(255),
        capital VARCHAR(255),
        population INT,
        area_km2 DECIMAL(10, 2),
        gdp_euro_million DECIMAL(20, 2),
        currency VARCHAR(255)
    )
""")

mydb.commit()
print("cleanDB tables ensured")

#Closing connection
cursor.close()
mydb.close()