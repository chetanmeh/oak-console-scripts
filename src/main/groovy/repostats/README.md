# Oak Repository Statistics

This script can be used to generate useful statistics related to repository content.
Generated stats would be dumped in text and json format in the launch directory.

This script can be used against any Oak version safely. It connects to the repository in read-only mode and 
it __does not require access to DataStore__. This script is resource intensive hence launch oak-run with higher memory

This would read the repo and generate 2 files which contains various stats related to repository content

* `repo-stats.json` - stats in json data
* `repo-stats.txt` - Stats in text form

Load url

    oak-run.jar console ":load https://github.com/chetanmeh/oak-console-scripts/blob/master/src/main/groovy/repostats/oakRepoStats.groovy"
    
* Read only
* Does not require DataStore
* See [readme](../../../../README.md#usage) for usage details 
* JDK Version 1.8 (mostly tested with 1.8.0_66)
