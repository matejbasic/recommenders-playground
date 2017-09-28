# Recommenders playground

Different recommendation algorithms based on CF, CBF and association analysis.

## Structure

### recommenders
Classes of different recommendation generation approaches:
 - content-based filtering
 - item-item collaborative filtering
 - user-user collaborative filtering
 - popular items
 - association analysis (apriori, ECLAT, FP-growth)

### analysis
Testers for assocation and other approaches (they output different structure) based on IR metrics (precision, recall, fallout, F1, specificity).

Visualizer class with different methods for displaying data distributions and metric plots.

### db
Two layers structure:
QueryManager deals with Cypher (Neo4j) queries and DataManager transforms data to appropriate format for further ussage.

### db_importer
Imports [data](https://github.com/matejbasic/recomm-ecommerce-datasets) to Neo4j database.

### helpers
Decorators and logger.


## Other

Part of master thesis "Recommender systems based on association analysis" (mentor: prof.dr.sc Božidar Kliček) @ FOI, Varaždin.

Other thesis related projects:
- [MDAR](https://github.com/matejbasic/MDAR)
- [used datasets](https://github.com/matejbasic/recomm-ecommerce-datasets)
