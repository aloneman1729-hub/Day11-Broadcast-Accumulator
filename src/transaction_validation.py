from pyspark import SparkContext, SparkConf
import csv


# --------------------------------------------------
# Load Product Master
# --------------------------------------------------

def load_products(products_file):

    with open(products_file, "r") as file:
        reader = csv.DictReader(file)

        product_map = {
            row["product_id"]: {
                "product_name": row["product_name"],
                "price": float(row["price"])
            }
            for row in reader
        }

    return product_map


# --------------------------------------------------
# Validate Transaction
# --------------------------------------------------

def validate_transaction(record, products, bad_records):

    transaction_id, product_id, quantity = record.split(",")

    if product_id not in products:

        bad_records.add(1)

        return (
            f"{transaction_id} | INVALID | "
            f"Product {product_id} does not exist"
        )

    product = products[product_id]

    total_amount = float(quantity) * product["price"]

    return (
        f"{transaction_id} | VALID | "
        f"{product['product_name']} | "
        f"Quantity={quantity} | "
        f"Total={total_amount:.2f}"
    )


# --------------------------------------------------
# Main Spark Application
# --------------------------------------------------

def main():

    # Create Spark Context

    conf = (
        SparkConf()
        .setAppName("TransactionValidation")
        .set("spark.hadoop.fs.defaultFS", "file:///")
    )

    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("WARN")


    # Load Product Master

    products_file = "data/products.csv"

    product_map = load_products(products_file)


    # Broadcast Product Master

    broadcast_products = sc.broadcast(product_map)


    # Create Accumulator

    bad_records = sc.accumulator(0)


    # Load Transactions

    transactions_file = "data/transactions.csv"

    transactions_rdd = (
        sc.textFile(transactions_file)
        .filter(lambda line: not line.startswith("transaction_id"))
    )


    # Validate Transactions

    validated_rdd = transactions_rdd.map(
        lambda record: validate_transaction(
            record,
            broadcast_products.value,
            bad_records
        )
    )


    # Trigger Spark Execution

    results = validated_rdd.collect()


    # Display Results

    print("\n===== TRANSACTION VALIDATION RESULTS =====")

    for result in results:
        print(result)


    # Display Summary

    print("\n===== SUMMARY =====")
    print(f"Total transactions : {len(results)}")
    print(f"Bad records        : {bad_records.value}")


    # Stop Spark

    sc.stop()


# --------------------------------------------------
# Entry Point
# --------------------------------------------------

if __name__ == "__main__":
    main()
