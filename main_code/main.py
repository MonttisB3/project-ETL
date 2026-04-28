#Imports
from extractdata import (
    get_connection,
    extract_from_DB,
    extract_from_csv,
    extract_from_json,
    extract_from_api_countries
)

from transformdata import (
    transform_customers,
    transform_products,
    transform_orders,
    dropped_message
)

from loaddata import (
    get_clean_connection,
    load_customers,
    load_products,
    load_orders,
    load_country_info
)

#Queries
CUSTOMERS_QUERY = """
    SELECT customer_id, name, age, location
    FROM customers
"""

PRODUCTS_QUERY = """
    SELECT product_id, name, category, price
    FROM products
"""

ORDERS_QUERY = """
    SELECT order_id, customer_id, product_id, quantity, payment_method, order_date
    FROM orders
"""

#Main ETL pipeline
def run_etl():
    print("\nStarting ETL pipeline")

    # --- Extract ---
    print("\n Extract")

    #rawDB source
    raw_conn = get_connection()

    raw_customers = extract_from_DB(raw_conn, CUSTOMERS_QUERY)
    raw_products = extract_from_DB(raw_conn, PRODUCTS_QUERY)
    raw_orders = extract_from_DB(raw_conn, ORDERS_QUERY)

    raw_conn.close()

    #csv and json sources
    csv_orders = extract_from_csv("data/orders_dirty.csv")
    json_customers = extract_from_json("data/customers_dirty.json")

    #Tag each record with its source
    for row in raw_customers:
        row["source"] = "rawDB"
    for row in json_customers:
        row["source"] = "json"
    for row in raw_products:
        row["source"] = "rawDB"
    for row in raw_orders:
        row["source"] = "rawDB"
        row["customer_source"] = "rawDB" #rawDB orders reference customers from rawDB
    for row in csv_orders:
        row["source"] = "csv"
        row["customer_source"] = "json" #csv orders reference customers json

    #Combining all sources
    all_customers = raw_customers + json_customers
    all_orders = raw_orders + csv_orders
    all_products = raw_products

    print(f"Total customers extracted: {len(all_customers)}")
    print(f"Total products extracted: {len(all_products)}")
    print(f"Total orders extracted: {len(all_orders)}")

    # API source
    COUNTRY_CODES = ["FI", "SE", "NO", "DK"]
    country_data = extract_from_api_countries(COUNTRY_CODES)
    print(f"Total countries extracted from APIs: {len(country_data)}")


    # --- Transform ---
    print("\n Transform")

    clean_customers, customer_temp_map = transform_customers(all_customers)
    clean_products, product_temp_map = transform_products(all_products)
    clean_orders = transform_orders(all_orders, customer_temp_map, product_temp_map)


    # --- Load ---
    print("\n Load")

    clean_conn = get_clean_connection()

    customer_id_map = load_customers(clean_conn, clean_customers)
    product_id_map = load_products(clean_conn, clean_products)

    # Remap orders from tuple keys to real cleanDB IDs
    final_orders = []
    dropped_orders = 0
    for order in clean_orders:
        customer_key = order["customer_id"]  # already a (source, old_id) tuple
        product_key = order["product_id"]

        if customer_key not in customer_id_map or product_key not in product_id_map:
            dropped_orders += 1
            continue

        final_orders.append({
            "customer_id": customer_id_map[customer_key],
            "product_id": product_id_map[product_key],
            "quantity": order["quantity"],
            "payment_method": order["payment_method"],
            "order_date": order["order_date"],
            "source": order.get("source", "unknown")
        })

    dropped_message(dropped_orders, "mapping in load phase") #Debuggin purposes
    load_orders(clean_conn, final_orders)
    load_country_info(clean_conn, country_data)
    clean_conn.close()
    print("\n ETL PIPELINE COMPLETE")


#Run
if __name__ == "__main__":
    run_etl()