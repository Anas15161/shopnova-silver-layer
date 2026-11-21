"""Test d'integration end-to-end pour le moteur de transformation PySpark (Phase 4)."""

import sys
if "/home/iceberg" not in sys.path:
    sys.path.append("/home/iceberg")

from pyspark.sql import SparkSession
from src.adapters.factory import SourceAdapterFactory
from src.schemas.manager import SchemaManager
from src.transformers.factory import TransformerFactory


def main():
    print("=" * 60)
    print("DEMARRAGE DU TEST E2E DU TRANSFORMATION ENGINE (PHASE 4)")
    print("=" * 60)

    spark = SparkSession.builder.appName("ShopNova-Transformers-Test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema_mgr = SchemaManager(schemas_dir="/home/iceberg/configs/schemas")
    json_adapter = SourceAdapterFactory.get_adapter("json", spark)

    # =========================================================================
    # 1. TEST ORDERS TRANSFORMER (Deduplication par LSN, CDC, Business Rules)
    # =========================================================================
    print("[1/3] Test du OrdersTransformer...")
    orders_schema = schema_mgr.load_schema("orders")
    df_raw_orders = json_adapter.read("/home/iceberg/data/bronze_sample/orders_cdc.json")
    
    # Étape 1 : Normalisation de schéma canonique
    df_canonical_orders = schema_mgr.validate_and_transform(df_raw_orders, orders_schema)
    initial_count = df_canonical_orders.count()
    print(f"  -> Événements bruts CDC avant déduplication: {initial_count}")

    # Étape 2 : Transformation métier & Déduplication par LSN
    orders_transformer = TransformerFactory.get_transformer("orders")
    df_silver_orders = orders_transformer.transform(df_canonical_orders)
    final_count = df_silver_orders.count()
    print(f"  -> Commandes uniques après déduplication par LSN: {final_count}")
    
    df_silver_orders.show(truncate=False)

    # Assertions critiques sur la déduplication :
    # Dans orders_cdc.json, la commande 2001 a deux événements (LSN 1001 'c' PENDING, puis LSN 1002 'u' PAID)
    # Après déduplication, il ne doit rester que 2 commandes distinctes (2001 et 2002)
    assert final_count == 2, f"Attendu 2 commandes uniques, obtenu {final_count}"

    order_2001 = df_silver_orders.filter(df_silver_orders.order_id == 2001).collect()[0]
    print(f"  -> Commande 2001 : Statut retenu = '{order_2001.status}' (LSN = {order_2001._source_lsn})")
    
    # Vérification de la Règle d'or CDC : le dernier LSN (1002) doit l'emporter et écraser le statut 'pending'
    assert order_2001._source_lsn == 1002, "Le LSN le plus récent (1002) doit être retenu"
    assert order_2001.status == "paid", "Le statut le plus récent doit être 'paid'"
    assert order_2001.is_paid is True, "Le flag enrichi is_paid doit être True"
    assert order_2001.created_date is not None, "La date de partitionnement doit être calculée"

    # =========================================================================
    # 2. TEST CUSTOMERS TRANSFORMER (Nettoyage, Title Case, Concaténation)
    # =========================================================================
    print("\n[2/3] Test du CustomersTransformer...")
    customers_schema = schema_mgr.load_schema("customers")
    # Création d'un dataset de test avec variations de casse et espaces
    raw_customers_data = [
        {"customer_id": 501, "first_name": "  anas  ", "last_name": "HADDOU", "email": "ANAS@EXAMPLE.COM", "phone": None, "city": "rabat", "created_at": "2026-11-20T10:00:00Z", "source": {"lsn": 10}, "op": "c"},
        {"customer_id": 501, "first_name": "Anas", "last_name": "Haddou", "email": "anas@example.com", "phone": "0600000000", "city": "Rabat", "created_at": "2026-11-20T10:00:00Z", "source": {"lsn": 20}, "op": "u"},
    ]
    df_raw_cust = spark.read.json(spark.sparkContext.parallelize([str(c).replace("'", '"').replace("None", "null") for c in raw_customers_data]))
    df_canonical_cust = schema_mgr.validate_and_transform(df_raw_cust, customers_schema)
    
    customers_transformer = TransformerFactory.get_transformer("customers")
    df_silver_cust = customers_transformer.transform(df_canonical_cust)
    df_silver_cust.show(truncate=False)

    cust_rows = df_silver_cust.collect()
    assert len(cust_rows) == 1, "Le client 501 doit être dédupliqué sur le dernier LSN (20)"
    assert cust_rows[0].first_name == "Anas"
    assert cust_rows[0].last_name == "Haddou"
    assert cust_rows[0].full_name == "Anas Haddou"
    assert cust_rows[0].email == "anas@example.com"
    assert cust_rows[0].phone == "0600000000"

    # =========================================================================
    # 3. TEST PRODUCTS TRANSFORMER (Catégorie normalisée, Stock flag)
    # =========================================================================
    print("\n[3/3] Test du ProductsTransformer...")
    products_schema = schema_mgr.load_schema("products")
    raw_products_data = [
        {"product_id": 10, "name": "Laptop Pro", "category": "  ELECTRONICS  ", "price": 999.99, "stock_quantity": 15, "created_at": "2026-11-20T10:00:00Z", "source": {"lsn": 1}, "op": "c"},
        {"product_id": 11, "name": "Out of Stock Item", "category": "Accessories", "price": 19.99, "stock_quantity": 0, "created_at": "2026-11-20T10:00:00Z", "source": {"lsn": 2}, "op": "c"},
    ]
    df_raw_prod = spark.read.json(spark.sparkContext.parallelize([str(p).replace("'", '"') for p in raw_products_data]))
    df_canonical_prod = schema_mgr.validate_and_transform(df_raw_prod, products_schema)
    
    products_transformer = TransformerFactory.get_transformer("products")
    df_silver_prod = products_transformer.transform(df_canonical_prod)
    df_silver_prod.show(truncate=False)

    prod_rows = df_silver_prod.collect()
    assert len(prod_rows) == 2
    assert prod_rows[0].category == "electronics", "La catégorie doit être en minuscules et trimmée"
    assert prod_rows[0].is_in_stock is True, "Le produit 10 est en stock (quantité > 0)"
    assert prod_rows[1].is_in_stock is False, "Le produit 11 est hors stock (quantité == 0)"

    print("\n" + "=" * 60)
    print("✅ TOUS LES TESTS DU TRANSFORMATION ENGINE ONT REUSSI !")
    print("Déduplication stricte par LSN, normalisations et enrichissements validés.")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
