import mysql.connector
import random
from faker import Faker
from datetime import date
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_HOST, DB_USER, DB_PASSWORD

fake = Faker()

#Constants
countries = ["Finland", "Sweden", "Norway", "Denmark"]
payment_methods = ["Debit", "Credit", "Cash", "Bank-transfer"]

customer_amount = 100
order_amount = 500

years = list(range(2010, 2021))
year_weights = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

def generate_year():
    chosen_year = random.choices(years, weights=year_weights)[0]
    clean_date = fake.date_between(
        start_date=date(chosen_year, 1, 1),
        end_date=date(chosen_year, 12, 31)
    )
    return clean_date

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

cursor.execute("USE rawDB")


#Creating functions for adding dummy data into rawDB tables
def customer_mock_data():
    sql = "INSERT INTO customers (name, age, location) VALUES (%s, %s, %s)"
    # countries = ["Finland", "Sweden", "Norway", "Denmark"]

    for _ in range(customer_amount):
        name = fake.name()
        age = int(random.gauss(35,15))
        age = max(18, min(99, age)) #Gaussian distribution to ages
        location = random.choices(countries, weights=[40,20,30,10])[0] #Weighted data
        val = (name, age, location)
        cursor.execute(sql, val)

    mydb.commit()
    print(f"{customer_amount} New data inserted into customers table")

def product_mock_data():
    sql = "INSERT INTO products (name, category, price) VALUES (%s, %s, %s)"
    products = [
        {"name": "Office Chair", "category": "Furniture", "price": 89.99},
        {"name": "Standing Desk", "category": "Furniture", "price": 299.99},
        {"name": "Laptop", "category": "Electronics", "price": 999.99},
        {"name": "Smartphone", "category": "Electronics", "price": 699.99},
        {"name": "Wireless Headphones", "category": "Electronics", "price": 199.99},
        {"name": "Notebook", "category": "Stationery", "price": 4.99},
        {"name": "Ballpoint Pen", "category": "Stationery", "price": 1.99},
        {"name": "Coffee Mug", "category": "Kitchen", "price": 12.99},
        {"name": "Water Bottle", "category": "Kitchen", "price": 15.99},
        {"name": "Desk Lamp", "category": "Furniture", "price": 39.99}
    ]

    for product in products:
        name = product["name"]
        category = product["category"]
        price = product["price"]
        val = (name, category, price)
        cursor.execute(sql, val)

    mydb.commit()
    print("10 New data inserted into products table")

def orders_mock_data():
    sql = "INSERT INTO orders (customer_id, product_id, quantity, payment_method, order_date) VALUES (%s, %s, %s, %s, %s)"
    # payment_methods = ["Debit", "Credit", "Cash", "Bank-transfer"]

    for _ in range(order_amount):
        customer_id = random.randint(1, 100) #!!!!!!!!!!!!!! Range must match ID amounts in customer tables
        product_id = random.randint(1,10) #!!!!!!!!!!!!!!! Range must match ID amounts in product tables
        quantity = random.randint(1,5)
        payment_method = random.choices(payment_methods, weights=[40,20,10,30,])[0] #Weighted data
        order_date = generate_year()
        val = (customer_id, product_id, quantity, payment_method, order_date)
        cursor.execute(sql, val)

    mydb.commit()
    print(f"{order_amount} New data created in orders table")

if __name__ == "__main__":
    customer_mock_data()
    product_mock_data()
    orders_mock_data()
    cursor.close()
    mydb.close()

