import csv
import json
import random
from faker import Faker
from datetime import date

fake = Faker()

#Constants
countries = ["Finland", "Sweden", "Norway", "Denmark"]
payment_methods = ["Debit", "Credit", "Cash", "Bank-transfer", "Card"] #Card is intentional uclean data

customer_amount_JSON = 100
order_amount_CSV = 200

years = list(range(2010, 2021))
year_weights = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

def generate_year():
    chosen_year = random.choices(years, weights=year_weights)[0]
    clean_date = fake.date_between(
        start_date=date(chosen_year, 1, 1),
        end_date=date(chosen_year, 12, 31)
    )
    return clean_date

#Generate messy customer JSON
def mock_customers_json():
    customers = []

    for i in range(1, customer_amount_JSON + 1):
        customer = {
            "customer_id": i,
            "name": fake.name() if random.random() > 0.05 else "",  # 5% missing names
            "age": random.randint(18, 99) if random.random() > 0.05 else "unknown",  # 5% unknown
            "location": random.choices(countries, weights=[40,20,30,10])[0]
        }
        customers.append(customer)


    with open("data/customers_dirty.json", "w") as f:
        json.dump(customers, f, indent=4)

    print(f"{customer_amount_JSON} customers saved to customers_dirty.json")

#Generate messy order CSV
def mock_orders_csv():
    with open("data/orders_dirty.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["customer_id", "product_id", "quantity", "payment_method", "order_date"])

        for _ in range(order_amount_CSV):
            customer_id = random.randint(1, 100)
            product_id = random.randint(1, 10)
            quantity = random.choices([1, 2, 3, "two", ""], weights=[30,30,30,2.5,2.5])[0] #5% dirty
            payment_method = random.choices(payment_methods, weights=[30,30,10,25,5])[0] #5& dirty
            order_date = random.choices([generate_year(), "2020-13-01", ""], weights=[95, 2.5, 2.5])[0] #5% unclean data
            writer.writerow([customer_id, product_id, quantity, payment_method, order_date])

    print(f"{order_amount_CSV} orders saved to orders_dirty.csv")

if __name__ == "__main__":
    mock_customers_json()
    mock_orders_csv()