# Guide d'Adaptabilité et de Découplage Bronze ➔ Silver

> **Projet** : ShopNova E-Commerce Data Lakehouse  
> **Rôle** : Ingénieur Couche Silver (Spark + Iceberg)  
> **Auteur** : Anas HADDOU — Élève-ingénieur en Génie Informatique (EMG Rabat)  
> **Encadrant** : M. KHALD  

---

## 1. Contexte & Problématique Industrielle

Dans une architecture de Data Lakehouse en entreprise ou en projet d'équipe, **la couche Bronze et la couche Silver sont développées par des équipes ou des ingénieurs distincts**. 

Une contrainte majeure du projet est la suivante :  
**Le responsable de la couche Bronze n'a pas encore stabilisé ni finalisé ses livrables (formats de fichiers, conventions de nommage, flux batch ou streaming CDC Debezium).**

### Le Risque Classique d'un Système Rigide
Si le code de la couche Silver contenait des appels en dur (*hardcoding*) comme :
```python
# ❌ ERREUR DE CONCEPTION : code rigide et fortement couplé
df = spark.read.csv("s3a://bronze/orders.csv", header=True)
orders = df.select("order_id", "total_amount", "created_at")
```
Alors la moindre décision du responsable Bronze (changement d'extension, passage au format Parquet, nommage `num_commande` au lieu de `order_id`, structure imbriquée Debezium `after.order_id`) casserait l'intégralité du pipeline et exigerait une réécriture complète du code.

---

## 2. L'Architecture Découplée : L'Adaptateur Universel

Pour immuniser la Silver Layer contre toute variation des données Bronze, nous avons conçu un **système en couches étanches** agissant comme un **adaptateur universel** :

```
┌────────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER (SOURCE)                           │
│ (Inconnue / Variable : CSV, Parquet, JSON lines, Debezium CDC Kafka)   │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Données brutes
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│               COUCHE D'ADAPTABILITE SILVER (ISOLATION)                 │
│                                                                        │
│   1. Source Adapters (Phase 2)       2. Schema Contracts YAML (Phase 3)│
│   ┌───────────────────────────┐      ┌─────────────────────────────┐   │
│   │ • CSVSourceAdapter        │      │ • configs/schemas/*.yaml    │   │
│   │ • JSONSourceAdapter (CDC) │─────▶│ • Mapping candidats fallback│   │
│   │ • ParquetSourceAdapter    │      │ • Casting strict des types  │   │
│   │ • IcebergSourceAdapter    │      │ • Injection métadonnées CDC │   │
│   └───────────────────────────┘      └─────────────────────────────┘   │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Données standardisées (Canoniques)
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                  CŒUR DU PIPELINE SILVER (INVARIANT)                   │
│                                                                        │
│   3. Transformations (Phase 4)       4. Data Quality (Phase 5)         │
│   ┌───────────────────────────┐      ┌─────────────────────────────┐   │
│   │ • Déduplication par LSN   │      │ • Détection des anomalies   │   │
│   │ • Soft deletes (op == 'd')│─────▶│ • Routage vers Quarantaine  │   │
│   │ • Standardisation métier  │      │ • Rapport d'audit qualité   │   │
│   └───────────────────────────┘      └─────────────────────────────┘   │
│                                   │                                    │
│                                   ▼                                    │
│   5. Persistance ACID (Phase 6)      6. Exposition REST (Phase 8)      │
│   ┌───────────────────────────┐      ┌─────────────────────────────┐   │
│   │ • Apache Iceberg MERGE    │─────▶│ • FastAPI Query Service     │   │
│   │ • Snapshots & Time Travel │      │ • Endpoints /orders, etc.   │   │
│   └───────────────────────────┘      └─────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Les 5 Piliers d'Adaptabilité du Système

### Pilier 1 : Adaptabilité aux Formats de Fichiers (Phase 2)
* **Composant** : `SourceAdapterFactory` et `BaseSourceAdapter`.
* **Fonctionnement** : Le pipeline ne sait pas et n'a pas besoin de savoir si la source est un fichier CSV, JSON, Parquet ou une table Iceberg Bronze.
* **Auto-détection** : La fabrique examine l'URI (`.csv`, `.json`, `.parquet`, ou `table_identifier`) et instancie l'adaptateur adéquat.

### Pilier 2 : Adaptabilité aux Nommages et Structures (Phase 3)
* **Composant** : Contrats déclaratifs `configs/schemas/*.yaml` et `SchemaManager`.
* **Principe du Fallback Mapping** : Chaque colonne Silver déclare une liste ordonnée de champs candidats :
  ```yaml
  - name: "order_id"
    source_fields: ["after.order_id", "before.order_id", "num_commande", "order_id"]
    type: "bigint"
    required: true
  ```
* **Résultat** :
  * Si Bronze envoie un événement CDC Debezium ➔ le champ est extrait depuis `after.order_id`.
  * Si Bronze envoie un fichier CSV classique ➔ le champ est extrait depuis `order_id`.
  * Si Bronze décide d'appeler la colonne `num_commande` ➔ il suffit d'ajouter `"num_commande"` dans le YAML **sans toucher une ligne de Python**.

### Pilier 3 : Adaptabilité aux Types de Données et Formats Temporels (Phase 3)
* **Composant** : `type_converter.py` et `SchemaManager.validate_and_transform()`.
* **Fonctionnement** :
  * Les chaînes de caractères représentant des montants (`"129.99"`) sont automatiquement castées en `DecimalType(12, 2)` pour éviter les erreurs d'arrondi.
  * Les dates aux formats ISO 8601 (`"2026-11-20T10:00:00Z"`), SQL (`"2026-11-20 10:00:00"`) ou millisecondes epoch sont unifiées vers `TimestampType()`.

### Pilier 4 : Adaptabilité au Désordre et aux Doublons (Phases 4 & 6)
* **Composants** : Déduplication par LSN (`source.lsn`) et `MERGE` Apache Iceberg.
* **Règle d'or CDC** : En environnement distribué, un événement ancien peut arriver après un événement récent. Le système ordonne les transactions selon le **LSN** (Log Sequence Number) de PostgreSQL et non par l'heure d'arrivée dans MinIO.
* **Idempotence** : Rejouer 10 fois le même lot de données Bronze ne crée aucun doublon dans la table Silver.

### Pilier 5 : Adaptabilité aux Données Corrompues (Phase 5)
* **Composant** : Quarantaine MinIO (`s3a://quarantine/`).
* **Fonctionnement** : Si Bronze livre des lignes corrompues (montant négatif, ID manquant, statut inconnu), le pipeline ne plante pas. Les lignes fautives sont automatiquement isolées en quarantaine avec le motif du rejet, préservant ainsi l'intégrité de la Silver Layer pour l'équipe Gold.

---

## 4. Protocole d'Intégration le Jour J (Checklist en 4 étapes)

Lorsque le responsable Bronze fournira son jeu de données final, voici la procédure exacte à suivre :

| Étape | Action | Fichier à modifier | Temps estimé |
| :---: | :--- | :--- | :---: |
| **1** | Inspecter un échantillon de la livraison Bronze (format, noms de colonnes) | Aucun | 2 min |
| **2** | Si les noms de champs diffèrent de la norme, ajouter les alias dans le mapping | `configs/schemas/<dataset>.yaml` | 2 min |
| **3** | Indiquer l'emplacement S3/MinIO source dans la configuration | `.env` ou paramètre d'exécution | 1 min |
| **4** | Lancer l'orchestrateur Silver | `make run` | 1 min |

**Total : moins de 10 minutes d'intégration, 0 ligne de code Spark modifiée.**

---

## 5. Conclusion pour la Soutenance

Ce découplage strict démontre l'application des meilleures pratiques de l'ingénierie des données moderne :
1. **Separation of Concerns (SoC)** : la transformation métier est indépendante de l'ingestion physique.
2. **Configuration over Hardcoding** : la flexibilité est pilotée par les métadonnées déclaratives YAML.
3. **Robustesse et Idempotence** : résilience aux variations, aux pannes et aux ré-exécutions.
