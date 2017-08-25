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
import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfoDocument
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore
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
    enum OakVersion {
        V_1_6("AEM 6.3/Oak 1.6.x"),
        V_1_4("AEM 6.2/Oak 1.4.x"),
        V_1_2("AEM 6.1/Oak 1.2.x"),
        V_1_0("AEM 6.0/Oak 1.0.x"),
        UNKNOWN("Unknown")

        final String desc

        private OakVersion(String desc) {
            this.desc = desc
        }
    }
    NodeStore nodeStore
    Whiteboard whiteboard
    String dbname
    OakVersion oakVersion
    boolean testMode = Boolean.getBoolean("testMode")

    def execute() {
        oakVersion = determineOakVersion()
        println "Oak version detected as ${oakVersion.desc}"
        dbname = determineDbName()

        Map<String, String> cps = asyncCheckpoints()
        println "Current checkpoints referred by Async indexer $cps"
        boolean checkpointMissing = false
        cps.each { name, cp ->
            NodeState cpState = nodeStore.retrieve(cp)
            if (cpState == null || testMode) {
                checkpointMissing = true
                println "[$name] Referred checkpoint $cp found to be missed"
                printCheckPointCommand(name, cp)
            }
        }

        if (!checkpointMissing) {
            println("All referred checkpoints are found to be valid. No command to invoke")
        }

    }

    OakVersion determineOakVersion() {
        DocumentNodeStore dns = nodeStore as DocumentNodeStore

        OakVersion ov = OakVersion.UNKNOWN
        ClusterNodeInfoDocument.all(dns.getDocumentStore()).each { doc ->
            String version = doc.get("oakVersion") as String
            if (ov == OakVersion.UNKNOWN && version != null) {
                println "Oak version from DB is $version"
                if (version.startsWith("1.6")){
                    ov = OakVersion.V_1_6
                } else if (version.startsWith("1.4")){
                    ov = OakVersion.V_1_4
                } else if (version.startsWith("1.2")){
                    ov = OakVersion.V_1_2
                } else if (version.startsWith("1.0")){
                    ov = OakVersion.V_1_0
                }
            }
        }

        return ov
    }

    String determineDbName() {
        MongoConnection mc = WhiteboardUtils.getService(whiteboard, MongoConnection.class)
        assert mc
        return mc.DB.name
    }

    def printCheckPointCommand(String name, String cp) {
        def expiry = String.valueOf(TimeUnit.DAYS.toMillis(1000))

        printCommand(OakVersion.V_1_0, createCommand(cp, expiry))

        def attrs = new LinkedHashMap() //Ensure that expires is first entry
        attrs['expires'] = expiry
        attrs['creator'] = 'AsyncIndexUpdate'
        attrs['thread'] = 'recoverCheckpoint'

        printCommand(OakVersion.V_1_2, createCommand(cp, JsonOutput.toJson(attrs)))

        //Recreate the map to ensure rv is second entry
        attrs = new LinkedHashMap()
        attrs['expires'] = expiry
        attrs['rv'] = createRevVector(cp) //Keep rev as second entry
        attrs['creator'] = 'AsyncIndexUpdate'
        attrs['thread'] = 'recoverCheckpoint'
        attrs['name'] = name
        printCommand(OakVersion.V_1_4, createCommand(cp, JsonOutput.toJson(attrs)))

        attrs['created'] = ISO8601.format(Calendar.getInstance())
        printCommand(OakVersion.V_1_6, createCommand(cp, JsonOutput.toJson(attrs)))
    }

    def createRevVector(String cp) {
        Revision r = Revision.fromString(cp)
        DocumentNodeState docState = nodeStore.root as DocumentNodeState

        def revs = []
        docState.rootRevision.each {Revision rr ->
            int clusterId = rr.clusterId
            long timeStamp = Math.min(r.timestamp, rr.timestamp)

            //For read only connection the root rev vector is min of dimension 2
            //with 1 from clusterId 0 so exclude that
            if (clusterId != 0) {
                revs << new Revision(timeStamp, r.counter, clusterId)
            }
        }
        return new RevisionVector(revs).toString()
    }

    private void printCommand(OakVersion ov, String command) {
        //oakVersion support was present in 1.0.19, 1.2.4 and 1.4 or newer
        //So unknown case is only for 1.0 and 1.2
        if (oakVersion == OakVersion.UNKNOWN && ( ov == OakVersion.V_1_0 || ov == OakVersion.V_1_2)){
            printCommand(ov.desc, command)
        } else if (oakVersion == ov){
            printCommand(ov.desc, command)
        }
    }

    private static void printCommand(String versionInfo, String command) {
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
