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


import com.google.common.base.Function
import com.google.common.collect.FluentIterable
import com.google.common.collect.Iterables
import com.google.common.collect.TreeTraverser
import groovy.transform.CompileStatic
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry
import org.apache.jackrabbit.oak.spi.state.NodeState
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils
import org.apache.jackrabbit.oak.spi.state.NodeStore

@CompileStatic
class ChildCounter{
    NodeStore nodeStore

    def count(){
        String path = System.getProperty("path")
        assert path : "Path not specified via 'path' system property"
        println "Counting children below $path"
        NodeState state = NodeStateUtils.getNode(nodeStore.root, path)
        def childCount = getTreeTraversor(state).size()
        println "Number of children under $path is $childCount"
    }

    static FluentIterable<NodeState> getTreeTraversor(NodeState state){
        def traversor = new TreeTraverser<NodeState>(){
            Iterable<NodeState> children(NodeState root) {
                return Iterables.transform(root.getChildNodeEntries(), {ChildNodeEntry cne -> cne.nodeState} as Function)
            }
        }
        return traversor.preOrderTraversal(state)
    }
}

new ChildCounter(nodeStore: session.store).count()


