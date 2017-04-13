# Index Consistency Checker

Checks consistency for all the Lucene indexes present in a repository.
This script would require access to the DataStore so that must be configured while launching `oak-run console`

Load url

    oak-run.jar console ":load https://github.com/chetanmeh/oak-console-scripts/blob/master/src/main/groovy/index/checkIndexConsistency.groovy"
