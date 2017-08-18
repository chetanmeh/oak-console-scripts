# Description

[OAK-6081] implemented oak-run based [indexing][oak-run-indexing]. Once
indexed data is created, it can be imported to an existing setup via
multiple [ways][import-index]. [OAK-6453] implements importing indexed
data via [script][import-via-script]. The said script is implemented by
[IndexImportScript.groovy].

# Requirements

* Oak repository running inside an Osgi container
* Oak version being used should be between 1.2.* and 1.6.* (inclusive)
* [Apache Felix Script Console Plugin][script-console-plugin]
* Groovy support [bundle][groovy-all]

# Usage

1. Do out of band indexing as described [here][oob-indexing]
1. Start up the setup where the index needs to be imported
1. Use `IndexStats` MBean to `abortAndPause()` all async lanes that
are to be imported
1. Verify that indexing is indeed paused by watching that there are no
more updates for those lanes.
1. Goto `/system/console/sc` and select language as `Groovy`
1. Paste contents of [IndexImportScript.groovy] in text area of
script console
1. Change `indexPath` value in 2nd last [line](IndexImportScript.groovy#L454)
to point to index generated in Step1
1. Execute the script
1. The output panel of script console should describe what the script
did and report error, if any
1. Verify that the index is caught up and results are consistent
1. Resume async indexing via `IndexStats` MBean for lanes which were
paused in Step3
1. Delete checkpoint created in Step1

# Disclaimer

The script has been tested with AEM setups running oak versions 1.0.0,
1.2.2, 1.4.1 and 1.6.1. For other setup (non-AEM) or other oak versions
(less than 1.2.0 or more than 1.6.*) YMMV.

[OAK-6081]: https://issues.apache.org/jira/browse/OAK-6081
[OAK-6453]: https://issues.apache.org/jira/browse/OAK-6453
[oak-run-indexing]: https://jackrabbit.apache.org/oak/docs/query/oak-run-indexing.html
[oob-indexing]: https://jackrabbit.apache.org/oak/docs/query/oak-run-indexing.html#out-of-band-indexing
[import-index]: https://jackrabbit.apache.org/oak/docs/query/oak-run-indexing.html#out-of-band-import-reindex
[import-via-script]: https://jackrabbit.apache.org/oak/docs/query/oak-run-indexing.html#import-index-script
[script-console-plugin]: http://felix.apache.org/documentation/subprojects/apache-felix-script-console-plugin.html
[groovy-all]: https://mvnrepository.com/artifact/org.codehaus.groovy/groovy-all/2.4.6
[IndexImportScript.groovy]: IndexImportScript.groovy