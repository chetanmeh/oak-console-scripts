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

package index



import com.google.common.base.Function
import com.google.common.base.Stopwatch
import com.google.common.collect.Iterables
import com.google.common.io.ByteStreams
import com.google.common.io.CountingInputStream
import groovy.transform.CompileStatic
import org.apache.commons.io.IOUtils
import org.apache.jackrabbit.JcrConstants
import org.apache.jackrabbit.oak.api.*
import org.apache.jackrabbit.oak.commons.PathUtils
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup
import org.apache.jackrabbit.oak.plugins.tree.RootFactory
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider
import org.apache.jackrabbit.oak.query.QueryEngineSettings
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider
import org.apache.jackrabbit.oak.query.ast.SelectorImpl
import org.apache.jackrabbit.oak.query.index.FilterImpl
import org.apache.jackrabbit.oak.spi.query.PropertyValues
import org.apache.jackrabbit.oak.spi.state.NodeState
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils
import org.apache.jackrabbit.oak.spi.state.NodeStore
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.jcr.PropertyType

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.DECLARING_NODE_TYPES
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE

@CompileStatic
class IndexConsistencyChecker {
    final Logger log = LoggerFactory.getLogger(getClass())

    NodeStore nodeStore
    int validIndexCount = 0
    int invalidIndexCount = 0

    def check(){
        Stopwatch w = Stopwatch.createStarted()
        def indexPaths = getIndexDefnPaths()
        Root root = RootFactory.createReadOnlyRoot(nodeStore.root)

        indexPaths.each {String path ->
            path = PathUtils.concat('/', path)
            Tree idx = root.getTree(path)
            if (idx.getProperty('type').getValue(Type.STRING) == 'lucene'){
                checkConsistency(idx)
            }
        }

        println "Consistency check completed in $w"
        if (invalidIndexCount){
            println "Found $invalidIndexCount corrupted index out of $totalIndexCount indexes"
        } else {
            println "All $totalIndexCount are found to be consistent"
        }
        return null
    }

    private int getTotalIndexCount() {
        invalidIndexCount + validIndexCount
    }

    IndexInfo checkConsistency(Tree t) {
        println "Checking ${t.path}"
        IndexInfo info = new IndexInfo(name:t.path)
        checkBinaryProps(t, info)

        if (!info.valid){
            println "Index ${t.path} is corrupt"
            invalidIndexCount++
        } else {
            validIndexCount++
            println "  ${info.toString()}"
        }

        return info
    }

    def checkBinaryProps(Tree tree, IndexInfo info) {
        tree.properties.each {PropertyState ps ->
            if (ps.getType().tag() == PropertyType.BINARY){
                if (ps.isArray()){
                    for (int i = 0; i < ps.count(); i++) {
                        Blob b = ps.getValue(Type.BINARY, i)
                        checkBlob(b, tree, info)
                    }
                } else {
                    Blob b = ps.getValue(Type.BINARY)
                    checkBlob(b, tree, info)
                }
            }
            info.indexFiles++
        }
        tree.getChildren().each {Tree child  ->
            checkBinaryProps(child, info)
        }
    }

    def checkBlob(Blob blob, Tree tree, IndexInfo info) {
        String id = blob.getContentIdentity()
        try{
            InputStream is = blob.newStream
            CountingInputStream cis = new CountingInputStream(is)
            IOUtils.copyLarge(cis, ByteStreams.nullOutputStream())

            if (cis.count != blob.length()){
                println "\t - Invalid blob $id. Length mismatch - expected ${blob.length()} -> found ${cis.count}"
                info.invalidBlobs << id
            }
            info.size += cis.count
        } catch (Exception e) {
            log.debug("Error occurred reading blob at {}", tree.path, e)
            info.invalidBlobs << id
            println "\t - Invalid blob $id"
        }
    }

    private Iterable<String> getIndexDefnPaths(){
        NodeState nodeType = NodeStateUtils.getNode(nodeStore.root, '/oak:index/nodetype')

        //Check if oak:QueryIndexDefinition is indexed as part of nodetype index
        if (Iterables.contains(nodeType.getNames(DECLARING_NODE_TYPES), INDEX_DEFINITIONS_NODE_TYPE)){
            println "Loading index definitions paths from NodeType index"
            PropertyIndexLookup pil = new PropertyIndexLookup(nodeStore.root)
            return pil.query(createFilter(INDEX_DEFINITIONS_NODE_TYPE),
                    JcrConstants.JCR_PRIMARYTYPE,
                    PropertyValues.newName([INDEX_DEFINITIONS_NODE_TYPE]))
        } else {
            //Else just check the root indexes
            Tree nodeTypeTree = TreeFactory.createReadOnlyTree(nodeType)
            return Iterables.transform(nodeTypeTree.children, {Tree t -> t.path} as Function)
        }
    }

    private FilterImpl createFilter(String nodeTypeName) {
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(nodeStore.root)
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName)
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName)
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings())
    }

    private static class IndexInfo {
        String name
        long size = 0
        List<String> invalidBlobs = []
        int indexFiles = 0

        boolean isValid(){
            return invalidBlobs.size() == 0
        }

        String toString(){
            return "Size : ${org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount(size)}"
        }
    }
}

new IndexConsistencyChecker(nodeStore: session.store).check()


