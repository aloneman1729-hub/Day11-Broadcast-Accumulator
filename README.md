# Day 11 — Broadcast and Accumulators

A PySpark project demonstrating how **Broadcast Variables** and **Accumulators** can be used to validate transaction data against a small product master dataset.

## Project Objective

The application validates transactions by checking whether each transaction contains a valid `product_id` from the product master.

It demonstrates two important Spark concepts:

- **Broadcast Variable** — efficiently shares small reference data with Spark executors.
- **Accumulator** — collects distributed metrics such as the number of invalid transactions.

## Project Structure

```text
Day11-Broadcast-Accumulator/
│
├── data/
│   ├── products.csv
│   └── transactions.csv
│
├── src/
│   └── transaction_validation.py
│
├── tests/
│   └── test_transaction_validation.py
│
├── requirements.txt
└── README.md
Input Data
Product Master

The product master contains:

Product ID	Product	Price
P101	Laptop	50000
P102	Mouse	1000
P103	Keyboard	2000
P104	Monitor	15000
P105	Headphones	3000
Transactions

The transaction file contains 10 transactions.

Three transactions contain invalid product IDs:

T003 → P999
T006 → P888
T009 → P777
How Broadcast Works

The product master is a small lookup dataset.

Instead of repeatedly sending the product information with every task, Spark creates a Broadcast Variable:

broadcast_products = sc.broadcast(product_map)

Executors can then access the product data using:

broadcast_products.value
Data Flow
                 DRIVER
                   │
                   │ Broadcast
                   ▼
            Product Master
                   │
          ┌────────┼────────┐
          ▼        ▼        ▼
       Executor Executor Executor
          │        │        │
          └────────┼────────┘
                   │
              Validation

Broadcast is useful when the reference dataset is relatively small and read-only.

How Accumulator Works

The project uses an accumulator to count invalid transactions:

bad_records = sc.accumulator(0)

When an invalid product is found:

bad_records.add(1)

The driver can then read the final value:

bad_records.value

Conceptually:

Executors
   │
   │ add(1)
   ▼
Accumulator
   │
   ▼
Driver
   │
   ▼
Bad records = 3

Accumulators are useful for counters and metrics.

They should not be used as the primary mechanism for maintaining business state.

Transaction Validation Logic

For every transaction:

Read the transaction_id, product_id, and quantity.
Check whether the product_id exists in the broadcast product master.
If the product does not exist:
Mark the transaction as INVALID.
Increment the accumulator.
If the product exists:
Calculate the total amount.
Mark the transaction as VALID.

Formula:

Total Amount = Quantity × Product Price
Running the Project
1. Activate the virtual environment
source .venv/bin/activate
2. Install dependencies
pip install -r requirements.txt
3. Run the Spark application
spark-submit src/transaction_validation.py
Expected Output
===== TRANSACTION VALIDATION RESULTS =====

T001 | VALID | Laptop | Quantity=2 | Total=100000.00
T002 | VALID | Mouse | Quantity=3 | Total=3000.00
T003 | INVALID | Product P999 does not exist
T004 | VALID | Keyboard | Quantity=2 | Total=4000.00
T005 | VALID | Monitor | Quantity=1 | Total=15000.00
T006 | INVALID | Product P888 does not exist
T007 | VALID | Headphones | Quantity=2 | Total=6000.00
T008 | VALID | Laptop | Quantity=1 | Total=50000.00
T009 | INVALID | Product P777 does not exist
T010 | VALID | Mouse | Quantity=5 | Total=5000.00

===== SUMMARY =====
Total transactions : 10
Bad records        : 3
Testing

The project includes Pytest unit tests covering:

Valid laptop transaction
Invalid product transaction
Valid mouse transaction

Run the tests with:

pytest tests/test_transaction_validation.py -v

Expected result:

3 passed
Technologies Used
Python 3.12
Apache Spark 3.5.6
PySpark 3.5.6
Pytest 9.1.1
Spark RDD
Broadcast Variables
Accumulators
Key Spark Concepts
Transformation

The project uses:

transactions_rdd.map(...)

Transformations are lazily evaluated.

Action

The following action triggers Spark execution:

validated_rdd.collect()

The validation logic is therefore executed when the action is called.

Important Concepts Learned
Broadcast Variable

Driver → Executors

Used to efficiently distribute small, read-only reference data.

Accumulator

Executors → Driver

Used to aggregate counters and metrics from distributed tasks.

RDD

An RDD is Spark's distributed collection abstraction that allows data to be processed across multiple workers.

Lazy Evaluation

Spark does not immediately execute transformations. Execution starts when an action such as collect() is called.

Project Result

The application successfully processed:

10 total transactions
7 valid transactions
3 invalid transactions

The three invalid transactions were detected using the broadcast product master, and the accumulator correctly reported:

Bad records : 3

