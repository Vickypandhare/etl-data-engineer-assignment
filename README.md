# Eastvantage Data Engineer Assignment

## Overview
Company XYZ promo sale analysis — extracts total quantities per item per customer (aged 18–35), omitting zero-purchase rows.

## Files
| File | Description |
|---|---|
| `create_db.py` | Creates and seeds `sales.db` with test data |
| `solution.py` | Main script — both SQL and Pandas solutions |
| `solution_sql.csv` | Output from SQL approach |
| `solution_pandas.csv` | Output from Pandas approach |

## How to Run

```bash
# 1. Create the database
python create_db.py

# 2. Run both solutions
python solution.py
```

## Schema
```
Customer(customer_id PK, age)
Sales(sales_id PK, customer_id FK)
Items(item_id PK, item_name)
Orders(order_id PK, sales_id FK, item_id FK, quantity)
```

## Business Rules Applied
- Age filter: 18 ≤ age ≤ 35
- NULL quantity → excluded (NULL = not purchased, per business rules)
- Rows where total quantity = 0 are excluded
- Quantities cast to INTEGER (no decimals)
- CSV delimiter: `;`

## Expected Output Format
```
Customer;Age;Item;Quantity
1;21;x;10
2;23;x;1
2;23;y;1
2;23;z;1
3;35;z;2
```

## Dependencies
- Python 3.x
- pandas
- sqlite3 (standard library)
