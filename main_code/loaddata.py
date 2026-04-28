import mysql.connector
from config import DB_HOST, DB_USER, DB_PASSWORD

#Function for establishing connectiont to cleanDB
def get_clean_connection():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database="cleanDB"
        )
        print (f"\nDatabase connection to: {connection}")
        return connection
    except mysql.connector.Error as e:
        print("Database connection error:", e)
        exit()


#Load customers
def load_customers(connection, customers):
    cursor = connection.cursor()

    #Upsert
    query = """
        INSERT INTO customers_clean (source, name, age, location)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            source = VALUES(source),
            name = VALUES(name),
            age = VALUES(age), 
            location = VALUES(location)
    """

    id_map = {} #Proper id map raw_id -> clean_id

    for clean in customers:
        cursor.execute(query, (
            clean["_source"],
            clean["name"],
            clean["age"],
            clean["location"]
        ))

        # Fetch new clean ID
        clean_id = cursor.lastrowid

        # If lastrowid is 0 the row already existed — fetch its real ID
        if clean_id == 0:
            cursor.execute("""
                SELECT customer_id FROM customers_clean
                WHERE name = %s AND age = %s AND location = %s
            """, (clean["name"], clean["age"], clean["location"]))
            result = cursor.fetchone()
            clean_id = result[0]

        id_map[(clean["_source"], clean["_old_id"])] = clean_id

    connection.commit()
    cursor.close()

    print(f"Loaded {len(customers)} customers")
    return id_map
 

#Load products
def load_products(connection, products):
    cursor = connection.cursor()

    #Upsert
    query = """
        INSERT INTO products_clean (source, name, category, price)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            category = VALUES(category),
            price = VALUES(price)
    """

    id_map = {}

    for clean in products:
        cursor.execute(query, (
            clean["_source"],
            clean["name"],
            clean["category"],
            clean["price"]
        ))

        # Fetch new clean ID
        clean_id = cursor.lastrowid

        # If lastrowid is 0 the row already existed — fetch its real ID
        if clean_id == 0:
            cursor.execute("""
                SELECT product_id FROM products_clean
                WHERE name = %s AND price = %s
            """, (clean["name"], clean["price"]))
            result = cursor.fetchone()
            clean_id = result[0]

        id_map[("rawDB", clean["_old_id"])] = clean_id

    connection.commit()
    cursor.close()

    print(f"Loaded {len(products)} products")
    return id_map


#Load orders
def load_orders(connection, orders):
    cursor = connection.cursor()

    #Upsert not used here to preserve historic data
    query = """
        INSERT IGNORE INTO orders_clean 
        (source, customer_id, product_id, quantity, payment_method, order_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    data = [
        (
            o.get("source", "unknown"),
            o["customer_id"],
            o["product_id"],
            o["quantity"],
            o["payment_method"],
            o["order_date"]
        )
        for o in orders
    ]

    cursor.executemany(query, data)

    connection.commit()
    cursor.close()

    print(f"Loaded {len(orders)} orders")

#Load countries
def load_country_info(connection, countries):
    cursor = connection.cursor()

    query = """
        INSERT INTO country_info 
            (country_code, country_name, capital, population, area_km2, gdp_euro_million, currency)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            capital = VALUES(capital),
            population = VALUES(population),
            area_km2 = VALUES(area_km2),
            gdp_euro_million = VALUES(gdp_euro_million),
            currency = VALUES(currency)
    """

    for country in countries:
        cursor.execute(query, (
            country["country_code"],
            country["country_name"],
            country["capital"],
            country["population"],
            country["area_km2"],
            country["gdp_euro_million"],
            country["currency"]
        ))

    connection.commit()
    cursor.close()
    print(f"Loaded {len(countries)} countries")