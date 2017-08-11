# Oak Console Scripts

This module hosts miscellaneous scripts which can be used with [oak-run][1]. 

## Provided Scripts

* [Repository Statistics](src/main/groovy/repostats) - Generates useful statistics report for repository content
* [Index Consistency Checker](src/main/groovy/index) - Checks consistency of Lucene indexes

## Usage

To make use of this script

## 1. Download latest oak-run

```
$ wget -O oak-run-1.6.0.jar 'https://search.maven.org/remotecontent?filepath=org/apache/jackrabbit/oak-run/1.6.0/oak-run-1.6.0.jar'
```
Or download it from [here][2]

## 2. Execute Script via oak-run console

Run console and load the script. Standard usage

```
java -jar oak-run*.jar console /path/to/segmentstore ":load https://raw.githubusercontent.com/chetanmeh/oak-console-scripts/master/src/main/groovy/repostats/oakRepoStats.groovy"
```

This would load the script and execute it and output would be dumped to console. Some scripts
may also generate some output files in the launch directory

Following are various variants

Usage with new segment tar (Oak >= 1.6)

    console /path/to/segmentstore
     
Usage with old segment (Oak < 1.6)
    
    console --segment=true /path/to/segmentstore
     
Usage with FileDataStore

    console -fds-path=/path/to/datastore /path/to/segmentstore
     
Usage with Mongo

    console mongodb://server:27017/dbname
    
**Running via ssh**

Set system property `-Djline.terminal=jline.UnsupportedTerminal` when running the command via `nohup`

```
$ nohup java -Xmx4g -Djline.terminal=jline.UnsupportedTerminal -jar oak-run*.jar console --segment=true /path/to/segmentstore ":load https://raw.githubusercontent.com/chetanmeh/oak-console-scripts/master/src/main/groovy/repostats/oakRepoStats.groovy" &
```

### S3DataStore

S3 support pending OAK-6077

 
[1]: https://github.com/apache/jackrabbit-oak/tree/trunk/oak-run#console
[2]: http://search.maven.org/remotecontent?filepath=org/apache/jackrabbit/oak-run/1.6.0/oak-run-1.6.0.jar
