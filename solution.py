"""
Eastvantage Data Engineer Assignment
=====================================
Company XYZ – Promo Sale Analysis
Extract total quantities per item per customer (aged 18–35).

Two solutions provided:
  1. Pure SQL
  2. Pandas

Output: semicolon-delimited CSV (solution_sql.csv / solution_pandas.csv)
"""

import sqlite3
import pandas as pd


DB_PATH = "sales.db"


# ─────────────────────────────────────────────
# DB Connection
# ─────────────────────────────────────────────
def get_connection():
    return sqlite3.connect(DB_PATH)


# ─────────────────────────────────────────────
# SOLUTION 1 – Pure SQL
# ─────────────────────────────────────────────
def solution_sql(conn) -> pd.DataFrame:
    """
    Uses a single SQL query to:
      - Join Sales → Customer (filter age 18–35)
      - Join Orders → Items
      - Exclude NULL quantities (NULL = not purchased, per business rules)
      - SUM quantity per customer per item
      - Filter out rows where total = 0
      - Cast to INTEGER (no decimals)
    """
    query = """
        SELECT
            c.customer_id   AS Customer,
            c.age           AS Age,
            i.item_name     AS Item,
            CAST(SUM(o.quantity) AS INTEGER) AS Quantity
        FROM Customer c
        JOIN Sales    s ON s.customer_id = c.customer_id
        JOIN Orders   o ON o.sales_id   = s.sales_id
        JOIN Items    i ON i.item_id    = o.item_id
        WHERE c.age BETWEEN 18 AND 35
          AND o.quantity IS NOT NULL
        GROUP BY c.customer_id, c.age, i.item_name
        HAVING SUM(o.quantity) > 0
        ORDER BY c.customer_id, i.item_name;
    """
    df = pd.read_sql_query(query, conn)
    return df


# ─────────────────────────────────────────────
# SOLUTION 2 – Pandas
# ─────────────────────────────────────────────
def solution_pandas(conn) -> pd.DataFrame:
    """
    Loads all four tables into DataFrames, then uses
    Pandas merge / groupby to replicate the same logic.
    NULL quantities are dropped — they mean 'not purchased',
    not 'zero purchased'.
    """
    # Load tables
    customers = pd.read_sql_query("SELECT * FROM Customer", conn)
    sales     = pd.read_sql_query("SELECT * FROM Sales",    conn)
    orders    = pd.read_sql_query("SELECT * FROM Orders",   conn)
    items     = pd.read_sql_query("SELECT * FROM Items",    conn)

    # Filter customers aged 18–35
    customers = customers[customers["age"].between(18, 35)]

    # Merge: Customer → Sales → Orders → Items
    df = (
        customers
        .merge(sales,  on="customer_id")
        .merge(orders, on="sales_id")
        .merge(items,  on="item_id")
    )

    # Exclude NULL quantities — NULL means not purchased (per business rules)
    df = df[df["quantity"].notna()]

    # Aggregate: sum per customer per item
    df = (
        df.groupby(["customer_id", "age", "item_name"], as_index=False)
          .agg(Quantity=("quantity", "sum"))
    )

    # Drop rows with total quantity = 0
    df = df[df["Quantity"] > 0].copy()

    # No decimals
    df["Quantity"] = df["Quantity"].astype(int)

    # Rename columns to match required output
    df.rename(columns={
        "customer_id": "Customer",
        "age":         "Age",
        "item_name":   "Item"
    }, inplace=True)

    df.sort_values(["Customer", "Item"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ─────────────────────────────────────────────
# Save to CSV (semicolon-delimited)
# ─────────────────────────────────────────────
def save_csv(df: pd.DataFrame, filename: str) -> None:
    df.to_csv(filename, sep=";", index=False)
    print(f"Saved → {filename}")
    print(df.to_string(index=False))
    print()


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    conn = None
    try:
        conn = get_connection()

        print("=" * 45)
        print("SOLUTION 1 – Pure SQL")
        print("=" * 45)
        df_sql = solution_sql(conn)
        save_csv(df_sql, "solution_sql.csv")

        print("=" * 45)
        print("SOLUTION 2 – Pandas")
        print("=" * 45)
        df_pandas = solution_pandas(conn)
        save_csv(df_pandas, "solution_pandas.csv")

        # Verify both outputs match
        match = df_sql.reset_index(drop=True).equals(df_pandas.reset_index(drop=True))
        print(f"Both solutions produce identical output: {match}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()
