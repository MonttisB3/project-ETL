import numpy as np
import pandas as pd
from scipy.optimize import minimize

from main_code.loaddata import get_clean_connection
from main_code.extractdata import extract_from_DB

# --- Queries ---
ORDERS_QUERY = """
    SELECT o.order_id, o.quantity, o.customer_id, o.source AS order_source,
           p.price, c.location
    FROM orders_clean o
    JOIN products_clean p ON o.product_id = p.product_id
    JOIN customers_clean c ON o.customer_id = c.customer_id
"""

COUNTRY_QUERY = """
    SELECT country_code, country_name, population
    FROM country_info
"""

# --- Extract ---
conn = get_clean_connection()
orders = extract_from_DB(conn, ORDERS_QUERY)
countries = extract_from_DB(conn, COUNTRY_QUERY)
conn.close()

# --- Prepare dataframes ---
orders_df = pd.DataFrame(orders)
countries_df = pd.DataFrame(countries)

#Convert decimal types to float
orders_df["quantity"] = orders_df["quantity"].astype(float)
orders_df["price"] = orders_df["price"].astype(float)

#Make a revenue column
orders_df["revenue"] = orders_df["quantity"] * orders_df["price"]



# --- Calculate revenue per country ---
revenue_by_country = (
    orders_df.groupby("location")["revenue"]
    .sum()
    .reset_index()
    .rename(columns={"location": "country_name"})
)

# --- Merge with population data ---
country_data = countries_df.merge(revenue_by_country, on="country_name")
country_data["revenue_per_capita"] = country_data["revenue"] / country_data["population"]

# Normalize efficiency scores to sum to 1
country_data["efficiency"] = (
    country_data["revenue_per_capita"] / country_data["revenue_per_capita"].sum()
)

print("\nCountry efficiency scores:")
print(country_data[["country_name", "revenue", "population", "efficiency"]])



# --- Optimization ---
TOTAL_BUDGET = 100000 #This can be changed to whatever budget
n = len(country_data)
efficiency = country_data["efficiency"].values

# Objective function — negative because scipy minimizes, we want to maximize
def objective(budgets):
    return -np.sum(efficiency * np.sqrt(budgets))

# Constraint: budgets must sum to total budget
constraints = {"type": "eq", "fun": lambda x: np.sum(x) - TOTAL_BUDGET}

# Bounds: each country gets at least 0
bounds = [(0, TOTAL_BUDGET)] * n

# Initial guess: split evenly
x0 = np.full(n, TOTAL_BUDGET / n)

# Run optimization
result = minimize(
    objective,
    x0,
    method="SLSQP",
    bounds=bounds,
    constraints=constraints,
    options={"ftol": 1e-12, "maxiter": 10000}
)

if not result.success:
    raise RuntimeError(f"Optimization failed: {result.message}")

# --- Results ---
country_data["optimal_budget"] = result.x
country_data["budget_share"] = country_data["optimal_budget"] / TOTAL_BUDGET * 100
country_data["expected_return_score"] = efficiency * np.sqrt(result.x)

country_data = country_data.sort_values("optimal_budget", ascending=False)

print("\n--- Optimal Marketing Budget Allocation ---")
print(f"Total budget: {TOTAL_BUDGET}€")
for _, row in country_data.iterrows():
    print(
        f"{row['country_name']}: "
        f"{row['optimal_budget']:.2f}€ "
        f"({row['budget_share']:.1f}%) "
        f"→ expected return score: {row['expected_return_score']:.4f}"
    )