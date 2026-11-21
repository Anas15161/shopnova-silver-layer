# Phase 6 — Apache Iceberg Storage & Table Management

Ce document détaille l'architecture, les spécifications techniques de stockage, les stratégies de partitionnement, les opérations ACID MERGE (Merge-On-Read), le Time Travel et les procédures de maintenance de la couche **Silver** pour le projet **ShopNova**.

---

## 1. Objectifs & Cadre Méthodologique

La Phase 6 constitue le cœur de persistance de la couche Silver dans le Lakehouse ShopNova :
1. **Catalogue REST Apache Iceberg (`lake`)** : découplage complet entre le catalogue de métadonnées (`http://rest:8181`) et le stockage objet MinIO S3 (`s3://warehouse/silver/...`).
2. **Format-Version 2 (Row-Level Deletes & Updates)** : prise en charge native des mutations CDC (updates, deletes) via la stratégie **Merge-On-Read (MOR)** sans réécriture lourde des fichiers Parquet de données (Copy-On-Write évité).
3. **Partitionnement Optimal** :
   * `silver.orders` : partitionné par jour de création (`created_date`) pour minimiser le pruning lors des requêtes d'analyse temporelle et des jointures Gold.
   * `silver.products` : partitionné par catégorie (`category`).
   * `silver.customers` : table de dimension gérée à plat (unpartitioned).
4. **Opérations ACID MERGE & Résilience CDC** :
   * MERGE idempotent basé sur les clés primaires métiers (`order_id`, `customer_id`, `product_id`).
   * **Garde LSN Stricte** : `WHEN MATCHED AND (source._source_lsn >= target._source_lsn OR target._source_lsn IS NULL) THEN UPDATE...`. Cette condition prévient toute régression silencieuse d'état lors de l'arrivée d'événements CDC en désordre.
   * Prise en compte immédiate des suppressions : `WHEN MATCHED AND source.is_deleted = true THEN DELETE`.
5. **Time Travel & Audit Historique** :
   * Navigation temporelle via `VERSION AS OF <snapshot_id>` ou `TIMESTAMP AS OF '<timestamp>'`.
   * Démonstration exigée par le cahier des charges : passage d'une commande de `paid` à `refunded`, suivie de la reconstruction de l'état `paid` antérieur via Time Travel.
6. **Maintenance & Compaction Automatisée** :
   * Réécriture et consolidation des petits fichiers Parquet (`CALL lake.system.rewrite_data_files`).
   * Expiration des anciens snapshots (`CALL lake.system.expire_snapshots`).

---

## 2. Architecture & Propriétés des Tables Silver

### Définition DDL des Tables (`lake.silver.*`)

#### `lake.silver.orders`
```sql
CREATE TABLE IF NOT EXISTS lake.silver.orders (
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    status STRING,
    total_amount DECIMAL(12, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    created_date DATE,
    is_paid BOOLEAN,
    is_final_status BOOLEAN,
    is_deleted BOOLEAN,
    _last_op STRING,
    _source_lsn BIGINT,
    _ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (created_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',
    'write.parquet.compression-codec' = 'zstd'
);
```

#### `lake.silver.customers`
```sql
CREATE TABLE IF NOT EXISTS lake.silver.customers (
    customer_id BIGINT NOT NULL,
    email STRING,
    first_name STRING,
    last_name STRING,
    full_name STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    is_deleted BOOLEAN,
    _last_op STRING,
    _source_lsn BIGINT,
    _ingested_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',
    'write.parquet.compression-codec' = 'zstd'
);
```

#### `lake.silver.products`
```sql
CREATE TABLE IF NOT EXISTS lake.silver.products (
    product_id BIGINT NOT NULL,
    name STRING,
    category STRING,
    price DECIMAL(10, 2),
    stock_quantity INT,
    is_in_stock BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    is_deleted BOOLEAN,
    _last_op STRING,
    _source_lsn BIGINT,
    _ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (category)
TBLPROPERTIES (
    'format-version' = '2',
    'write.merge.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.delete.mode' = 'merge-on-read',
    'write.parquet.compression-codec' = 'zstd'
);
```

---

## 3. Implémentation du CDC MERGE Idempotent

Le module `src/storage/iceberg_writer.py` génère dynamiquement et exécute la requête `MERGE INTO` :

```sql
MERGE INTO lake.silver.orders AS target
USING _staging_orders_batch AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.is_deleted = true THEN
    DELETE
WHEN MATCHED AND (
    source._source_lsn >= target._source_lsn OR target._source_lsn IS NULL
) THEN
    UPDATE SET target.status = source.status,
               target.total_amount = source.total_amount,
               target.updated_at = source.updated_at,
               target.is_paid = source.is_paid,
               target.is_final_status = source.is_final_status,
               target._last_op = source._last_op,
               target._source_lsn = source._source_lsn,
               target._ingested_at = source._ingested_at
WHEN NOT MATCHED AND (source.is_deleted = false OR source.is_deleted IS NULL) THEN
    INSERT (order_id, customer_id, status, total_amount, created_at, updated_at,
            created_date, is_paid, is_final_status, is_deleted, _last_op, _source_lsn, _ingested_at)
    VALUES (source.order_id, source.customer_id, source.status, source.total_amount,
            source.created_at, source.updated_at, source.created_date, source.is_paid,
            source.is_final_status, source.is_deleted, source._last_op, source._source_lsn, source._ingested_at);
```

---

## 4. Time Travel & Reconstruction d'État

Grâce au commit log d'Iceberg et au client léger `IcebergTimeTravel` :
* Chaque opération `MERGE`, `APPEND` ou `DELETE` génère un nouveau `snapshot` immutable sous `s3://warehouse/silver/orders/metadata/`.
* La requête suivante permet d'inspecter l'état exact au snapshot 1 (avant remboursement) :
  ```sql
  SELECT order_id, status, total_amount
  FROM lake.silver.orders VERSION AS OF 8290261529435408446
  WHERE order_id = 4001;
  -- Résultat garanti : status = 'paid'
  ```
* Et l'état courant :
  ```sql
  SELECT order_id, status, total_amount
  FROM lake.silver.orders
  WHERE order_id = 4001;
  -- Résultat garanti : status = 'refunded'
  ```

---

## 5. Procédures de Maintenance

La classe `IcebergTableManager` expose des fonctions directes pour exécuter les procédures cataloguées d'Iceberg :
1. **Compaction des Petits Fichiers** :
   ```python
   table_mgr.rewrite_data_files("orders")
   # Appelle: CALL lake.system.rewrite_data_files(table => 'silver.orders')
   ```
2. **Expiration des Snapshots** :
   ```python
   table_mgr.expire_snapshots("orders", "2026-11-01 00:00:00")
   # Appelle: CALL lake.system.expire_snapshots(table => 'silver.orders', older_than => TIMESTAMP '2026-11-01 00:00:00')
   ```
