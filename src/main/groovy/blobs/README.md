# Blob id dumper

Dumps the blob id and path mapping for all blobs found referred on the head version


Load url for the script

    java -jar oak-run*.jar console /path/to/segmentstore ":load https://raw.githubusercontent.com/chetanmeh/oak-console-scripts/master/src/main/groovy/blobs/dumpBlobIdsAndPaths.groovy"

* Read only access
* Does not requires DataStore access
* See [readme](../../../../README.md#usage) for usage details 