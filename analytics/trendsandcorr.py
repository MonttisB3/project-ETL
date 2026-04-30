import pandas as pd
import matplotlib.pyplot as plt

from main_code.loaddata import get_clean_connection
from main_code.extractdata import extract_from_DB


#---QUERY---
ORDERS_QUERY = """
    SELECT 
        o.order_id,
        o.customer_id,
        o.product_id,
        o.quantity,
        o.payment_method,
        o.order_date,
        c.location,
        p.name AS product_name,
        p.category,
        p.price
    FROM orders_clean o
    JOIN customers_clean c ON o.customer_id = c.customer_id
    JOIN products_clean p ON o.product_id = p.product_id
"""


#---LOAD DATA---
conn = get_clean_connection()
orders = extract_from_DB(conn, ORDERS_QUERY)
conn.close()

df = pd.DataFrame(orders)


#---PREPARE DATA---
df["quantity"] = df["quantity"].astype(float)
df["price"] = df["price"].astype(float)
df["order_date"] = pd.to_datetime(df["order_date"])

df["revenue"] = df["quantity"] * df["price"]
df["year"] = df["order_date"].dt.year
df["month"] = df["order_date"].dt.month



#---PAYMENT METHOD TREND OVER TIME---
payment_by_year = (
    df.groupby(["year", "payment_method"])["order_id"]
    .count()
    .unstack(fill_value=0)
)

plt.figure(figsize=(12, 6))

for method in payment_by_year.columns:
    plt.plot(
        payment_by_year.index,
        payment_by_year[method],
        marker="o",
        label=method
    )

plt.title("Payment Method Usage Over Time")
plt.xlabel("Year")
plt.ylabel("Number of Orders")
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()


#---PRODUCT REVENUE BY YEAR---
product_year_revenue = (
    df.groupby(["year", "product_name"])["revenue"]
    .sum()
    .unstack(fill_value=0)
)

plt.figure(figsize=(12, 7))

for product in product_year_revenue.columns:
    plt.plot(
        product_year_revenue.index,
        product_year_revenue[product],
        marker="o",
        label=product
    )

plt.title("Product Revenue by Year")
plt.xlabel("Year")
plt.ylabel("Revenue (€)")
plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()


#---CORRELATION MATRIX---
correlation_columns = [
    "quantity",
    "price",
    "revenue",
    "year",
    "month"
]

corr_matrix = df[correlation_columns].corr()

print("\nCorrelation matrix:")
print(corr_matrix)

plt.figure(figsize=(8, 6))
plt.imshow(corr_matrix, aspect="auto")
plt.colorbar(label="Correlation coefficient")
plt.xticks(range(len(correlation_columns)), correlation_columns, rotation=45)
plt.yticks(range(len(correlation_columns)), correlation_columns)

for i in range(len(correlation_columns)):
    for j in range(len(correlation_columns)):
        plt.text(
            j,
            i,
            f"{corr_matrix.iloc[i, j]:.2f}",
            ha="center",
            va="center"
        )

plt.title("Correlation Matrix")
plt.tight_layout()
plt.show()
