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

import com.google.common.base.Stopwatch
import com.google.common.collect.FluentIterable
import com.google.common.collect.TreeTraverser
import groovy.transform.CompileStatic
import org.apache.jackrabbit.oak.api.Blob
import org.apache.jackrabbit.oak.api.PropertyState
import org.apache.jackrabbit.oak.api.Tree
import org.apache.jackrabbit.oak.api.Type
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory
import org.apache.jackrabbit.oak.spi.state.NodeStore

import javax.jcr.PropertyType

@CompileStatic
class BlobIdDumper {
    private static final String IN_MEM_BLOB_PREFIX = "0x"
    String fileName = "blob-paths.txt"
    NodeStore nodeStore
    long blobCount
    long nodeCount


    void dump(){
        Stopwatch w = Stopwatch.createStarted()
        File file = new File(fileName)
        Tree tree = TreeFactory.createReadOnlyTree(nodeStore.root)
        def itr = getTreeTraversor(tree)

        println "Writing blobId to path mapping to $file"

        file.withPrintWriter { pw ->
            for (Tree t in itr) {
                dumpBinaryProps(pw, t)
            }
        }

        println "Total $blobCount external blobs found in $nodeCount nodes in $w"
    }

    private void dumpBinaryProps(PrintWriter pw, Tree tree){
        nodeCount++
        tree.properties.each { PropertyState ps ->
            if (ps.getType().tag() == PropertyType.BINARY){
                if (ps.isArray()){
                    for (int i = 0; i < ps.count(); i++) {
                        Blob b = ps.getValue(Type.BINARY, i)
                        writeBlobPath(pw, b, tree)
                    }
                } else {
                    Blob b = ps.getValue(Type.BINARY)
                    writeBlobPath(pw, b, tree)
                }
            }
        }

        if (nodeCount % 10000 == 0) {
            println "Traversed $nodeCount so far ..."
        }
    }

    private void writeBlobPath(PrintWriter pw, Blob blob, Tree tree) {
        String id = blob.contentIdentity
        if (id != null && !id.startsWith(IN_MEM_BLOB_PREFIX)) {
            pw.printf("%s|%s%n", id, tree.path)
            blobCount++

            if (blobCount % 1000 == 0) {
                println "Found $blobCount so far ..."
            }
        }
    }

    private static FluentIterable<Tree> getTreeTraversor(Tree t){
        def traversor = new TreeTraverser<Tree>(){
            Iterable<Tree> children(Tree root) {
                return root.children
            }
        }
        return traversor.preOrderTraversal(t)
    }
}

new BlobIdDumper(nodeStore: session.store).dump()

