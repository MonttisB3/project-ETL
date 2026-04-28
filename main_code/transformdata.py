from datetime import datetime

#Constants and helping functions

valid_locations = {"Finland", "Sweden", "Norway", "Denmark"}
valid_payments = {"Credit", "Debit", "Cash", "Bank-Transfer"}


#Makes each string start with a capital letter
def normalize_text(value):
    if not value or not isinstance(value, str):
        return None
    return value.strip().title()

def parse_int(value):
    try:
        return int(value)
    except(ValueError, TypeError):
        return None

def parse_float(value):
    try:
        return float(value)
    except(ValueError, TypeError):
        return None

#Parse date and ensure it follows the same date format yyyy.mm.dd
def parse_date(value):
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except(ValueError, TypeError):
        return None


#Clean customer data
def transform_customers(raw_customers):
    clean_customers = []
    id_map = {} #Id map is used to map old id to new id

    dropped_name = 0
    dropped_age = 0
    dropped_location = 0

    for row in raw_customers:
        name = normalize_text(row.get("name"))
        age = parse_int(row.get("age"))
        location = normalize_text(row.get("location"))

        #Validation for clean data. Not valid rows is dropped
        if not name:
            dropped_name += 1
            continue
        if age is None or age < 18 or age > 99:
            dropped_age += 1
            continue
        if location not in valid_locations:
            dropped_location += 1
            continue

        source = row.get("source", "unknown")
        old_id = parse_int(row.get("customer_id"))

        clean_row = {
            "name": name,
            "age": age,
            "location": location,
            "_source": source,   # carried for load layer
            "_old_id": old_id
        }
        clean_customers.append(clean_row)

        if old_id is not None:
            id_map[(source, old_id)] = (source, old_id)

    dropped_message(dropped_name, "name")
    dropped_message(dropped_age, "age")
    dropped_message(dropped_location, "location")
    print(f"Customers after transform: {len(clean_customers)}")
    return clean_customers, id_map


#Clean product data
def transform_products(raw_products):
    clean_products = []
    id_map = {}

    dropped_name = 0
    dropped_category = 0
    dropped_price = 0

    for row in (raw_products):
        name = normalize_text(row.get("name"))
        category = normalize_text(row.get("category"))
        price = parse_float(row.get("price"))

        #Validatoin for clean data. Not valid rows is dropped
        if not name:
            dropped_name += 1
            continue
        if not category:
            dropped_category += 1
            continue
        if price is None or price < 0:
            dropped_price += 1
            continue

        old_id = parse_int(row.get("product_id"))

        clean_row = {
            "name": name,
            "category": category,
            "price": round(price, 2),
            "_source": "rawDB",  # products always from rawDB
            "_old_id": old_id
        }

        clean_products.append(clean_row)

        if old_id is not None:
            id_map[("rawDB", old_id)] = ("rawDB", old_id)

    dropped_message(dropped_name, "name")
    dropped_message(dropped_category, "category")
    dropped_message(dropped_price, "price")
    print(f"Products after transform: {len(clean_products)}")
    return clean_products, id_map


#Clean orders data
def transform_orders(raw_orders, customer_id_map, product_id_map):
    clean_orders = []

    dropped_customer = 0
    dropped_product = 0
    dropped_quantity = 0
    dropped_payment = 0
    dropped_date = 0


    for row in raw_orders:
        source = row.get("source", "unknown")
        customer_source = row.get("customer_source", source)
        raw_customer_id = parse_int(row.get("customer_id"))
        raw_product_id = parse_int(row.get("product_id"))

        customer_key = (customer_source, raw_customer_id)
        product_key = ("rawDB", raw_product_id)

        # Id validation
        if customer_key not in customer_id_map:
            dropped_customer += 1
            continue
        if product_key not in product_id_map:
            dropped_product += 1
            continue


        quantity = parse_int(row.get("quantity"))
        payment = normalize_text(row.get("payment_method"))
        order_date = parse_date(row.get("order_date"))


        #Rest of the validation
        if quantity is None or quantity < 1:
            dropped_quantity += 1
            continue
        if payment not in valid_payments:
            dropped_payment += 1
            continue
        if order_date is None:
            dropped_date += 1
            continue

        clean_row = {
            "customer_id": customer_key,
            "product_id": product_key,
            "quantity": quantity,
            "payment_method": payment,
            "order_date": order_date,
            "source": source
        }

        clean_orders.append(clean_row)

    dropped_message(dropped_customer, "customer id")
    dropped_message(dropped_product, "product id")
    dropped_message(dropped_quantity, "quantity")
    dropped_message(dropped_payment, "payment")
    dropped_message(dropped_date, "date")
    print(f"Orders after transform: {len(clean_orders)}")
    return clean_orders

def dropped_message(dropped_value, category):
    if dropped_value > 0:
        print(f"Dropped due to {category}:", dropped_value)
