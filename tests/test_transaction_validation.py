from pathlib import Path
import sys

from pyspark import SparkContext, SparkConf

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from transaction_validation import load_products, validate_transaction


PRODUCTS_FILE = "data/products.csv"


def create_test_context():
    conf = (
        SparkConf()
        .setMaster("local[2]")
        .setAppName("TransactionValidationTests")
        .set("spark.hadoop.fs.defaultFS", "file:///")
    )

    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("ERROR")

    return sc


def test_valid_transaction():

    sc = create_test_context()

    products = load_products(PRODUCTS_FILE)
    bad_records = sc.accumulator(0)

    result = validate_transaction(
        "T001,P101,2",
        products,
        bad_records
    )

    assert "VALID" in result
    assert "Laptop" in result
    assert "Total=100000.00" in result
    assert bad_records.value == 0

    sc.stop()


def test_invalid_product():

    sc = create_test_context()

    products = load_products(PRODUCTS_FILE)
    bad_records = sc.accumulator(0)

    result = validate_transaction(
        "T003,P999,1",
        products,
        bad_records
    )

    assert "INVALID" in result
    assert "P999" in result
    assert bad_records.value == 1

    sc.stop()


def test_mouse_transaction():

    sc = create_test_context()

    products = load_products(PRODUCTS_FILE)
    bad_records = sc.accumulator(0)

    result = validate_transaction(
        "T010,P102,5",
        products,
        bad_records
    )

    assert "VALID" in result
    assert "Mouse" in result
    assert "Total=5000.00" in result
    assert bad_records.value == 0

    sc.stop()
