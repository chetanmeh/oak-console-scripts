# Index Consistency Checker

Checks consistency for all the Lucene indexes present in a repository.

**Deprecated - Use [Oak Run Index Consistency Check](https://jackrabbit.apache.org/oak/docs/query/oak-run-indexing.html#check-index)**

Load url

    java -jar oak-run*.jar console /path/to/segmentstore console ":load https://github.com/chetanmeh/oak-console-scripts/blob/master/src/main/groovy/index/checkIndexConsistency.groovy"

* Read only access
* Requires DataStore access
* See [readme](../../../../README.md#usage) for usage details 
