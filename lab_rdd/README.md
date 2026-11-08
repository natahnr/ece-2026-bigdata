# Lab RDD PySpark

Exécution : placer `users.csv` et `orders.csv` dans le dossier courant, puis `python lab_rdd.py`.

## Résultats
- Partitions par défaut : 2 (users et orders), 8 après `repartition(8)`
- `orders_data.count()` : 5000
- Users : 102 lignes brutes, 50 utilisateurs après filtre UUID
- Déduplication : 5000 -> 5000 (aucun doublon)
- Orphelins avec faux user_id (`leftOuterJoin`) : 1

## Réponses
- Partitions : minPartitions (2 par défaut) et découpage du fichier en splits.
- `repartition` fait un shuffle, `coalesce` non (quand on diminue).
- Un 2e `count()` rejoue le lineage (nouveau job) sauf si `cache()` est posé.
- `textFile()` coupe sur `\n` sans notion de guillemets, d'où les adresses cassées.
- Une virgule en trop dans `product` provoque un `ValueError` à l'action.
- `reduceByKey` combine localement avant le shuffle, `groupByKey` shuffle toutes les paires.
- La jointure évite le shuffle si les deux RDD ont le même partitioner.
- `leftOuterJoin` garde les commandes sans user avec `None`.
- `collect()` sur gros volume risque un OutOfMemory sur le driver.
- Cache : sur 5000 lignes l'écart de temps est dans le bruit de mesure.
