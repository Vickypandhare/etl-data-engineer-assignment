import sqlite3

conn = sqlite3.connect("sales.db")
cur = conn.cursor()

# Create tables
cur.executescript("""
DROP TABLE IF EXISTS Orders;
DROP TABLE IF EXISTS Sales;
DROP TABLE IF EXISTS Customer;
DROP TABLE IF EXISTS Items;

CREATE TABLE Customer (
    customer_id INTEGER PRIMARY KEY,
    age INTEGER
);

CREATE TABLE Sales (
    sales_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

CREATE TABLE Items (
    item_id INTEGER PRIMARY KEY,
    item_name TEXT
);

CREATE TABLE Orders (
    order_id INTEGER PRIMARY KEY,
    sales_id INTEGER,
    item_id INTEGER,
    quantity INTEGER,
    FOREIGN KEY (sales_id) REFERENCES Sales(sales_id),
    FOREIGN KEY (item_id) REFERENCES Items(item_id)
);
""")

# Seed Items
cur.executemany("INSERT INTO Items VALUES (?, ?)", [
    (1, 'x'), (2, 'y'), (3, 'z')
])

# Seed Customers (mix of ages inside and outside 18-35)
cur.executemany("INSERT INTO Customer VALUES (?, ?)", [
    (1, 21),   # in range
    (2, 23),   # in range
    (3, 35),   # in range (boundary)
    (4, 17),   # out of range
    (5, 40),   # out of range
])

# Seed Sales
cur.executemany("INSERT INTO Sales VALUES (?, ?)", [
    (1, 1), (2, 1), (3, 1),   # customer 1 has 3 receipts
    (4, 2),                    # customer 2 has 1 receipt
    (5, 3), (6, 3),            # customer 3 has 2 receipts
    (7, 4),                    # customer 4 (underage)
    (8, 5),                    # customer 5 (overage)
])

# Seed Orders
# Customer 1: buys item x multiple times, others NULL
cur.executemany("INSERT INTO Orders VALUES (?, ?, ?, ?)", [
    (1,  1, 1, 4),    # sale 1: item x=4
    (2,  1, 2, None), # sale 1: item y=NULL (not bought)
    (3,  1, 3, None), # sale 1: item z=NULL
    (4,  2, 1, 3),    # sale 2: item x=3
    (5,  2, 2, None),
    (6,  2, 3, None),
    (7,  3, 1, 3),    # sale 3: item x=3
    (8,  3, 2, None),
    (9,  3, 3, None),
    # Customer 1 total: x=10, y=0, z=0

    (10, 4, 1, 1),    # customer 2: one of each
    (11, 4, 2, 1),
    (12, 4, 3, 1),
    # Customer 2 total: x=1, y=1, z=1

    (13, 5, 1, None), # customer 3: only buys z
    (14, 5, 2, None),
    (15, 5, 3, 1),    # sale 5: z=1
    (16, 6, 1, None),
    (17, 6, 2, None),
    (18, 6, 3, 1),    # sale 6: z=1
    # Customer 3 total: x=0, y=0, z=2

    (19, 7, 1, 5),    # customer 4 (age 17 - should be excluded)
    (20, 8, 1, 5),    # customer 5 (age 40 - should be excluded)
])

conn.commit()
conn.close()
print("Database created successfully.")
