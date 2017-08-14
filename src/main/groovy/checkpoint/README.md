# Recover lost checkpoint

This script checks if any of the checkpoints referred by AsyncIndexUpdate has been lost. If yes then it 
generates the Mongo command to recreate the checkpoint. 

Mongo command is generated for each supported Oak version. User should use the command as per his AEM setup

    java -jar oak-run*.jar console mongodb://server:27001/dbname ":load https://raw.githubusercontent.com/chetanmeh/oak-console-scripts/master/src/main/groovy/checkpoint/recoverCheckpoint.groovy"
    

* Must be used with oak-run-1.7.5+
* Safe to use against older AEM setup
* Read only access
* Does not requires DataStore access
* See [readme](../../../../README.md#usage) for usage details 
 