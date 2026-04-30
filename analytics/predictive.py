import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, r2_score

from main_code.loaddata import get_clean_connection
from main_code.extractdata import extract_from_DB

PRODUCTS_QUERY_PRICE = """
    SELECT product_id, price
    FROM products_clean
"""

ORDERS_QUERY_QUANTITY = """
    SELECT order_id, product_id, quantity, order_date
    FROM orders_clean
"""

# Extract data from cleanDB
conn = get_clean_connection()
orders = extract_from_DB(conn, ORDERS_QUERY_QUANTITY)
products = extract_from_DB(conn, PRODUCTS_QUERY_PRICE)
conn.close()

# Create dataframes and revenue column
ordersdf = pd.DataFrame(orders)
productsdf = pd.DataFrame(products)

ordersdf["quantity"] = pd.to_numeric(ordersdf["quantity"], errors="coerce")
productsdf["price"] = pd.to_numeric(productsdf["price"], errors="coerce")

df = ordersdf.merge(productsdf, on="product_id")
df["revenue"] = pd.to_numeric(df["quantity"] * df["price"], errors="coerce")

# Aggregate to monthly revenue
df["order_date"] = pd.to_datetime(df["order_date"])

monthly_revenue = (
    df.groupby(df["order_date"].dt.to_period("M"))["revenue"]
    .sum()
    .reset_index()
)

monthly_revenue["order_date"] = monthly_revenue["order_date"].dt.to_timestamp()
monthly_revenue["revenue"] = pd.to_numeric(monthly_revenue["revenue"], errors="coerce")

# Fill missing months using interpolation
all_months = pd.date_range(
    start=monthly_revenue["order_date"].min(),
    end=monthly_revenue["order_date"].max(),
    freq="MS"
)

monthly_revenue = (
    monthly_revenue.set_index("order_date")
    .reindex(all_months)
    .interpolate(method="time")
    .rename_axis("order_date")
    .reset_index()
)

# Convert date to numeric index
monthly_revenue["time_index"] = range(len(monthly_revenue))

# Hold out last 12 months for testing
split_index = len(monthly_revenue) - 12
train = monthly_revenue[:split_index]
test = monthly_revenue[split_index:]

#Linear regression up to the last 12 months and testing for accuracy
model = LinearRegression()
model.fit(train[["time_index"]], train["revenue"])

monthly_revenue["predicted_revenue"] = model.predict(monthly_revenue[["time_index"]])

test_predictions = model.predict(test[["time_index"]])
mae = mean_absolute_error(test["revenue"], test_predictions)
r2 = r2_score(test["revenue"], test_predictions)

print(f"\n--- Model Evaluation (on held-out last 12 months) ---")
print(f"MAE:  {mae:.2f}€  (on average, predictions are off by this many euros)")
print(f"R²:   {r2:.4f}  (1.0 = perfect, 0.0 = no better than guessing the mean)")

# Forecast next 12 months
future_steps = 12
future_index = range(len(monthly_revenue), len(monthly_revenue) + future_steps)

last_date = monthly_revenue["order_date"].max()
future_dates = pd.date_range(
    start=last_date + pd.offsets.MonthBegin(1),
    periods=future_steps,
    freq="MS"
)

future_df = pd.DataFrame({
    "order_date": future_dates,
    "time_index": future_index
})

future_df["predicted_revenue"] = model.predict(future_df[["time_index"]])

plt.figure(figsize=(12, 6))

# Training data
plt.plot(train["order_date"], train["revenue"],
         label="Actual Revenue (train)", color="steelblue")

# Test data — shown separately so split is visible
plt.plot(test["order_date"], test["revenue"],
         label="Actual Revenue (test)", color="orange")

# Full trend line
plt.plot(monthly_revenue["order_date"], monthly_revenue["predicted_revenue"],
         label="Trend Line", color="green", linestyle="--")

# Future forecast
plt.plot(future_df["order_date"], future_df["predicted_revenue"],
         label="Forecast", color="red", linestyle="--")

# Mark the train/test boundary
split_date = test["order_date"].iloc[0]
plt.axvline(x=split_date, color="gray", linestyle=":", label="Train/Test split")

plt.xlabel("Date")
plt.ylabel("Revenue (€)")
plt.title(f"Monthly Revenue Forecast  |  MAE: {mae:.2f}€  |  R²: {r2:.4f}")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()