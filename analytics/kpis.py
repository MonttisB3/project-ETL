import pandas as pd
import matplotlib.pyplot as plt

from main_code.loaddata import get_clean_connection
from main_code.extractdata import extract_from_DB


#---QUERIES---

ORDERS_QUERY = """
    SELECT 
        o.order_id,
        o.customer_id,
        o.product_id,
        o.quantity,
        o.order_date,
        c.location,
        p.price
    FROM orders_clean o
    JOIN customers_clean c ON o.customer_id = c.customer_id
    JOIN products_clean p ON o.product_id = p.product_id
"""

COUNTRY_QUERY = """
    SELECT country_name, population
    FROM country_info
"""


#---LOAD DATA---
conn = get_clean_connection()
orders = extract_from_DB(conn, ORDERS_QUERY)
countries = extract_from_DB(conn, COUNTRY_QUERY)
conn.close()

orders_df = pd.DataFrame(orders)
countries_df = pd.DataFrame(countries)


#---PREPARE DATA---
orders_df["quantity"] = orders_df["quantity"].astype(float)
orders_df["price"] = orders_df["price"].astype(float)
orders_df["order_date"] = pd.to_datetime(orders_df["order_date"])

orders_df["revenue"] = orders_df["quantity"] * orders_df["price"]
orders_df["year"] = orders_df["order_date"].dt.year


#---KPI CALCULATIONS---
latest_year = orders_df["year"].max()
previous_year = latest_year - 1

latest_data = orders_df[orders_df["year"] == latest_year]
previous_data = orders_df[orders_df["year"] == previous_year]

latest_customers = latest_data["customer_id"].nunique()
previous_customers = previous_data["customer_id"].nunique()
customer_growth = ((latest_customers - previous_customers) / previous_customers) * 100
customer_growth_target = 10

latest_revenue = latest_data["revenue"].sum()
previous_revenue = previous_data["revenue"].sum()
revenue_growth = ((latest_revenue - previous_revenue) / previous_revenue) * 100
revenue_growth_target = 15

latest_orders = latest_data["order_id"].nunique()
previous_orders = previous_data["order_id"].nunique()

latest_aov = latest_revenue / latest_orders
previous_aov = previous_revenue / previous_orders
aov_growth = ((latest_aov - previous_aov) / previous_aov) * 100
aov_target = 5

revenue_by_country = (
    orders_df.groupby("location")["revenue"]
    .sum()
    .reset_index()
    .rename(columns={"location": "country_name"})
)

country_data = countries_df.merge(revenue_by_country, on="country_name", how="left")
country_data["revenue"] = country_data["revenue"].fillna(0)
country_data["revenue_per_capita"] = country_data["revenue"] / country_data["population"]

denmark_rpc = country_data.loc[
    country_data["country_name"] == "Denmark", "revenue_per_capita"
].values[0]

denmark_rpc_target = denmark_rpc * 1.10


#---DASHBOARD DATA---
revenue_per_year = orders_df.groupby("year")["revenue"].sum()
customers_per_year = orders_df.groupby("year")["customer_id"].nunique()

country_data = country_data.sort_values("revenue_per_capita", ascending=False)


#---HELPER FUNCTION FOR KPI CARDS
def draw_kpi_card(ax, title, value, target, achieved):
    ax.axis("off")

    status = "Achieved" if achieved else "Below target"

    ax.text(
        0.5, 0.75,
        title,
        ha="center",
        va="center",
        fontsize=12,
        fontweight="bold"
    )

    ax.text(
        0.5, 0.48,
        value,
        ha="center",
        va="center",
        fontsize=18,
        fontweight="bold"
    )

    ax.text(
        0.5, 0.25,
        f"Target: {target}",
        ha="center",
        va="center",
        fontsize=10
    )

    ax.text(
        0.5, 0.08,
        status,
        ha="center",
        va="center",
        fontsize=10
    )

    for spine in ax.spines.values():
        spine.set_visible(True)


#---CREATE DASHBOARD---
fig = plt.figure(figsize=(16, 10))
fig.suptitle(
    f"KPI Dashboard - Business Performance ({latest_year})",
    fontsize=20,
    fontweight="bold"
)

grid = fig.add_gridspec(
    nrows=3,
    ncols=4,
    height_ratios=[1, 2, 2],
    hspace=0.45,
    wspace=0.35
)


# KPI cards
ax1 = fig.add_subplot(grid[0, 0])
draw_kpi_card(
    ax1,
    "Customer Growth",
    f"{customer_growth:.1f}%",
    "+10%",
    customer_growth >= customer_growth_target
)

ax2 = fig.add_subplot(grid[0, 1])
draw_kpi_card(
    ax2,
    "Revenue Growth",
    f"{revenue_growth:.1f}%",
    "+15%",
    revenue_growth >= revenue_growth_target
)

ax3 = fig.add_subplot(grid[0, 2])
draw_kpi_card(
    ax3,
    "AOV Growth",
    f"{aov_growth:.1f}%",
    "+5%",
    aov_growth >= aov_target
)

ax4 = fig.add_subplot(grid[0, 3])
draw_kpi_card(
    ax4,
    "Denmark Rev/Capita",
    f"{denmark_rpc:.6f}",
    f"{denmark_rpc_target:.6f}",
    denmark_rpc >= denmark_rpc_target
)


# Revenue trend chart
ax5 = fig.add_subplot(grid[1, :2])
ax5.plot(revenue_per_year.index, revenue_per_year.values, marker="o")
ax5.set_title("Revenue by Year")
ax5.set_xlabel("Year")
ax5.set_ylabel("Revenue (€)")
ax5.grid(True, alpha=0.3)


# Customer trend chart
ax6 = fig.add_subplot(grid[1, 2:])
ax6.plot(customers_per_year.index, customers_per_year.values, marker="o")
ax6.set_title("Customers by Year")
ax6.set_xlabel("Year")
ax6.set_ylabel("Unique Customers")
ax6.grid(True, alpha=0.3)


# Revenue per capita by country
ax7 = fig.add_subplot(grid[2, :])
bars = ax7.bar(
    country_data["country_name"],
    country_data["revenue_per_capita"]
)

ax7.set_title("Revenue per Capita by Country")
ax7.set_xlabel("Country")
ax7.set_ylabel("Revenue per Capita")

# Highlight Denmark label visually with annotation
denmark_row = country_data[country_data["country_name"] == "Denmark"]

if not denmark_row.empty:
    denmark_value = denmark_row["revenue_per_capita"].iloc[0]
    denmark_position = list(country_data["country_name"]).index("Denmark")

    ax7.annotate(
        "Focus country: Denmark",
        xy=(denmark_position, denmark_value),
        xytext=(denmark_position, denmark_value * 1.15),
        ha="center",
        arrowprops=dict(arrowstyle="->")
    )

plt.tight_layout()
plt.show()