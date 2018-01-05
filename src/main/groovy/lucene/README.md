Index Content Dumper
====================

Dumps lucene index contents in a file named `index-contents.txt`. This can be used to logically
compare 2 index dumps. This is done by un inverting the index in memory. So ensure that for large indexes
the processing is given sufficient heap 

    java -DindexPath=/path/to/lucene/index -jar oak-run*.jar console /path/to/segmentstore ":load https://raw.githubusercontent.com/chetanmeh/oak-console-scripts/master/src/main/groovy/lucene/luceneIndexDumper.groovy"
    
Where 

* `DindexPath` - System property specifying the Lucene index directory path

The `index-contents.txt` file contains the index information per document per line sorted by index path

```
/content/dam/catalogs/blob-store-stats.png|:ancestors,:depth,:fulltext,:nullProps,:path,full:jcr:content/metadata/dc:format,jcr:content/jcr:lastModified,jcr:content/metadata/dam:sha1,jcr:content/metadata/dam:size,jcr:content/metadata/dc:format,jcr:content/metadata/tiff:ImageLength,jcr:content/metadata/tiff:ImageWidth
/content/dam/catalogs/consolidated-listener-stats-2.png|:ancestors,:depth,:fulltext,:nullProps,:path,full:jcr:content/metadata/dc:format,jcr:content/jcr:lastModified,jcr:content/metadata/dam:sha1,jcr:content/metadata/dam:size,jcr:content/metadata/dc:format,jcr:content/metadata/tiff:ImageLength,jcr:content/metadata/tiff:ImageWidth
/content/dam/catalogs/consolidated-listener-stats.png|:ancestors,:depth,:fulltext,:nullProps,:path,full:jcr:content/metadata/dc:format,jcr:content/jcr:lastModified,jcr:content/metadata/dam:sha1,jcr:content/metadata/dam:size,jcr:content/metadata/dc:format,jcr:content/metadata/tiff:ImageLength,jcr:content/metadata/tiff:ImageWidth
/content/dam/catalogs/copy-on-read-stats.png|:ancestors,:depth,:fulltext,:nullProps,:path,full:jcr:content/metadata/dc:format,jcr:content/jcr:lastModified,jcr:content/metadata/dam:sha1,jcr:content/metadata/dam:size,jcr:content/metadata/dc:format,jcr:content/metadata/tiff:ImageLength,jcr:content/metadata/tiff:ImageWidth
```

Here each line is of following form

    <document path>|<comma seprated list of sorted field names>