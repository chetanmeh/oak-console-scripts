/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import groovy.json.JsonOutput
import org.apache.jackrabbit.oak.commons.json.JsopBuilder
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState
import org.apache.jackrabbit.oak.plugins.document.Revision
import org.apache.jackrabbit.oak.plugins.document.RevisionVector
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection
import org.apache.jackrabbit.oak.spi.state.NodeState
import org.apache.jackrabbit.oak.spi.state.NodeStore
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils
import org.apache.jackrabbit.util.ISO8601

import java.util.concurrent.TimeUnit

class CheckpointRecovery {
    NodeStore nodeStore
    Whiteboard whiteboard
    int noOfClusterNodes = System.getProperty("clusterNodeCount", "-1") as int
    String dbname

    def execute() {
        noOfClusterNodes = computeNoOfClusterNodes()
        println "Number of cluster nodes found $noOfClusterNodes"

        dbname = determineDbName()

        Map<String, String> cps = asyncCheckpoints()
        println "Current checkpoints referred by Async indexer $cps"
        boolean checkpointMissing = false
        cps.each { name, cp ->
            NodeState cpState = nodeStore.retrieve(cp)
            if (cpState == null) {
                checkpointMissing = true
                println "[$name] Referred checkpoint $cp found to be missed"
                printCheckPointCommand(name, cp)
            }
        }

        if (!checkpointMissing) {
            println("All referred checkpoints are found to be valid. No command to invoke")
        }

    }

    String determineDbName() {
        MongoConnection mc = WhiteboardUtils.getService(whiteboard, MongoConnection.class)
        assert mc
        return mc.DB.name
    }

    def computeNoOfClusterNodes() {
        DocumentNodeState dns = nodeStore.root as DocumentNodeState
        int count = 0
        dns.rootRevision.each {Revision r -> if (r.clusterId != 0) count++}
        return count
    }

    def printCheckPointCommand(String name, String cp) {
        def expiry = String.valueOf(TimeUnit.DAYS.toMillis(300))

        printCommand("AEM 6.0/Oak 1.0.x", createCommand(cp, expiry))

        def attrs = new LinkedHashMap() //Ensure that expires is first entry
        attrs['expires'] = expiry
        attrs['creator'] = 'AsyncIndexUpdate'
        attrs['thread'] = 'sling-oak-3-Registered Service.826'

        printCommand("AEM 6.1/Oak 1.2.x", createCommand(cp, JsonOutput.toJson(attrs)))

        //Recreate the map to ensure rv is second entry
        attrs = new LinkedHashMap()
        attrs['expires'] = expiry
        attrs['rv'] = createRevVector(cp) //Keep rev as second entry
        attrs['creator'] = 'AsyncIndexUpdate'
        attrs['thread'] = 'sling-oak-3-Registered Service.826'
        attrs['name'] = name
        printCommand("AEM 6.2/Oak 1.4.x", createCommand(cp, JsonOutput.toJson(attrs)))

        attrs['created'] = ISO8601.format(Calendar.getInstance())
        printCommand("AEM 6.3/Oak 1.6.x", createCommand(cp, JsonOutput.toJson(attrs)))
    }

    def createRevVector(String cp) {
        Revision r = Revision.fromString(cp)
        def revs = (1..noOfClusterNodes).collect { clusterId -> new Revision(r.timestamp, r.counter, clusterId) }
        return new RevisionVector(revs).toString()
    }

    def printCommand(String versionInfo, String command) {
        println "Command for version : $versionInfo"
        println(command)
        println()
    }

    def createCommand(String cp, String value) {
        def encoded = JsopBuilder.encode(value)
        """db.getSiblingDB("$dbname").settings.update({_id:"checkpoint"}, {\$inc:{_modCount:1}, \$set : {"data.$cp" : $encoded}})"""
    }

    Map<String, String> asyncCheckpoints() {
        NodeState state = nodeStore.root.getChildNode(":async")
        def checkpoints = [:]
        if (state.hasProperty("async")) {
            checkpoints['async'] = state.getString('async')
        }

        if (state.hasProperty("fulltext-async")) {
            checkpoints['fulltext-async'] = state.getString('fulltext-async')
        }

        return checkpoints
    }
}

new CheckpointRecovery(nodeStore: session.store, whiteboard: session.whiteboard).execute()
