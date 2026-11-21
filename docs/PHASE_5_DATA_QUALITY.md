# Phase 5 — Data Quality Framework & Quarantine Storage

Ce document présente l'architecture, la configuration déclarative et le fonctionnement du framework de qualité des données (Data Quality Engine) et du mécanisme d'isolation en Quarantaine pour la couche **Silver** de la plateforme **ShopNova**.

---

## 1. Objectifs & Cadre Méthodologique

Dans une architecture moderne Medallion Lakehouse, la couche **Silver** a pour responsabilité fondamentale d'agir comme un filtre d'intégrité strict : aucune donnée corrompue, incohérente ou silencieusement erronée ne doit atteindre les tables consommées par la couche Gold ou les modèles analytiques.

Le **Data Quality Framework** de ShopNova répond aux exigences suivantes :
1. **Couverture des 5 dimensions de la qualité de la donnée** :
   * **Complétude (Completeness)** : absence de valeurs nulles sur les attributs critiques (`order_id`, `customer_id`, `created_at`, etc.).
   * **Unicité (Uniqueness)** : absence de doublons fonctionnels après réconciliation CDC.
   * **Validité (Validity)** : conformité aux référentiels métiers (énumérations de statuts, emails valides, montants >= 0).
   * **Cohérence (Consistency)** : intégrité logique et temporelle (`updated_at >= created_at`, etc.).
   * **Précision (Accuracy)** : plausibilité statistique et économique (plafonds réalistes de montants ou prix).
2. **Définition Déclarative (YAML)** : les règles sont externalisées dans `configs/quality_rules/` sous forme d'expressions SQL PySpark, sans modification de code source Spark lors de l'ajout ou de l'ajustement d'une règle.
3. **Niveaux de Sévérité** :
   * `ERROR` : anomalie critique provoquant l'expulsion immédiate de l'enregistrement vers la table/stockage de **Quarantaine**.
   * `WARNING` : anomalie non-bloquante signalée dans le rapport d'audit et les métriques de monitoring sans bloquer le flux de données.
   * `INFO` : observation informative.
4. **Quarantine Storage ACID** : les enregistrements rejetés sont persistés avec traçabilité complète (`_rejection_reason`, `_rejected_at`, partitionnés par `quarantined_date`) dans le namespace Iceberg `lake.quarantine` sur MinIO S3.
5. **Rapport d'Audit & Métriques** : production systématique d'un rapport structuré (`QualityReport`) détaillant le nombre de lignes analysées, valides, rejetées, le taux de conformité (`pass_rate`) et le résultat par règle.

---

## 2. Architecture des Composants

```
                    ┌────────────────────────────┐
                    │    DataFrame Transformé    │
                    │      (Post-Phase 4)        │
                    └─────────────┬──────────────┘
                                  │
                                  ▼
                    ┌────────────────────────────┐
                    │     DataQualityEngine      │
                    │   (Evaluation déclarative) │
                    └──────┬──────────────┬──────┘
                           │              │
             Valid Rows    │              │ Rejected Rows (ERROR)
             (Conformes)   │              │ (Tags: _rejection_reason)
                           ▼              ▼
                    ┌────────────┐  ┌────────────────────────────┐
                    │  valid_df  │  │      QuarantineManager     │
                    │ (Vers Phase│  │ (lake.quarantine.<table_n>)│
                    │   6 Iceberg│  └─────────────┬──────────────┘
                    │   Storage) │                │
                    └────────────┘                ▼
                                            ┌───────────┐
                                            │ MinIO S3  │
                                            │ (Parquet) │
                                            └───────────┘
```

### Modules Développés

* **`src/quality/models.py`** :
  * Modèles Pydantic v2 : `DimensionEnum`, `SeverityEnum`, `QualityRule`, `QualityConfig`, `RuleResult`, `QualityReport`.
  * Validation automatique des types et sérialisation JSON pour intégration API et monitoring.
* **`src/quality/quarantine.py`** :
  * Classe `QuarantineManager` gérant l'écriture des rejets soit dans une table gérée Iceberg (`lake.quarantine.<table_name>`) soit dans un chemin Parquet avec partitionnement par date `quarantined_date`.
* **`src/quality/engine.py`** :
  * Classe `DataQualityEngine` : chargement des contrats YAML, vectorisation des expressions booléennes Spark SQL, calcul en une seule passe des colonnes d'anomalies, scission des DataFrames `valid_df` et `quarantined_df`, et génération du rapport `QualityReport`.
* **Règles déclaratives (`configs/quality_rules/`)** :
  * `orders_quality.yaml` : 7 règles (complétude, statuts, montants positifs, chronologie, seuils).
  * `customers_quality.yaml` : 6 règles (complétude, format regex email, noms non vides, dates valides).
  * `products_quality.yaml` : 6 règles (complétude, prix >= 0, stock >= 0, statut actif/inactif).

---

## 3. Exemple de Règle Déclarative YAML

Extrait de `configs/quality_rules/orders_quality.yaml` :

```yaml
table_name: orders
version: 1.0.0
rules:
  - name: order_id_not_null
    dimension: completeness
    severity: ERROR
    condition: "order_id IS NOT NULL"
    description: "L'identifiant unique de la commande est obligatoire."

  - name: valid_status
    dimension: validity
    severity: ERROR
    condition: "status IN ('pending', 'paid', 'shipped', 'delivered', 'cancelled', 'refunded')"
    description: "Le statut de commande doit respecter la nomenclature métier autorisée."

  - name: positive_total_amount
    dimension: validity
    severity: ERROR
    condition: "is_deleted = true OR (total_amount IS NOT NULL AND total_amount >= 0)"
    description: "Le montant total d'une commande active ne peut pas être strictement négatif."

  - name: chronological_consistency
    dimension: consistency
    severity: ERROR
    condition: "updated_at IS NULL OR updated_at >= created_at"
    description: "La date de mise à jour ne peut être antérieure à la date de création."

  - name: realistic_amount
    dimension: accuracy
    severity: WARNING
    condition: "is_deleted = true OR total_amount <= 500000.00"
    description: "Alerte de sécurité sur montant unitaire anormalement élevé."
```

---

## 4. Isolation et Traçabilité en Quarantaine

Lorsqu'une ou plusieurs règles de sévérité `ERROR` échouent sur une ligne :
1. Le moteur agrège les motifs sous forme de chaîne formatée :
   `"validity:positive_total_amount; consistency:chronological_consistency"`
2. Il adjoint l'horodatage de rejet `_rejected_at` (UTC) et la partition `quarantined_date`.
3. Il persiste l'enregistrement dans `lake.quarantine.orders`.
4. La ligne est immédiatement soustraite du flux `valid_df`, garantissant l'intégrité absolue de la table Iceberg cible `lake.silver.orders`.

Les équipes Data et métiers peuvent interroger la table de quarantaine directement par SQL :
```sql
SELECT order_id, status, total_amount, _rejection_reason, _rejected_at
FROM lake.quarantine.orders
WHERE quarantined_date = current_date()
ORDER BY _rejected_at DESC;
```

---

## 5. Validation et Exécution

* **Tests Unitaires (Hôte)** : validation des modèles Pydantic, sérialisation du rapport et parsing des fichiers YAML via `pytest tests/unit/test_quality.py`.
* **Test d'Intégration End-to-End (Spark Container)** : simulation de 6 commandes avec injection ciblée de 3 anomalies (montant négatif, statut inconnu, horodatage incohérent), vérification du routage vers la quarantaine et vérification de la persistance Iceberg sur MinIO.
  ```bash
  make test-quality
  ```
