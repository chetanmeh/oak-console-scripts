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


import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import com.google.common.base.Function
import com.google.common.base.Stopwatch
import com.google.common.collect.FluentIterable
import com.google.common.collect.Iterables
import com.google.common.collect.TreeTraverser
import groovy.transform.CompileStatic
import org.apache.jackrabbit.oak.commons.PathUtils
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry
import org.apache.jackrabbit.oak.spi.state.NodeState
import org.apache.jackrabbit.oak.spi.state.NodeStore
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils

@CompileStatic
class NodeStoreTraversalRateEstimator {
    final NodeStore store
    final Meter meter
    final Stopwatch watch = Stopwatch.createStarted()
    long traversalCount = 0

    NodeStoreTraversalRateEstimator(Whiteboard wb, NodeStore ns){
        this.store = ns
        MetricRegistry registry = WhiteboardUtils.getService(wb, MetricRegistry.class)
        assert registry : "Use --metrics option to enable metrics for this script"
        meter = registry.meter("traversal-meter")
    }

    def readAll(){
        getTreeTraversor(store.root).each{ SimpleTree t ->
            tick(t)
        }
        println "Done traversal of $traversalCount"
    }

    def tick(SimpleTree t){
        meter.mark()
        String id = t.path

        if (++traversalCount % 10000 == 0) {
            double rate = meter.getFiveMinuteRate()

            String formattedRate = String.format("%1.2f nodes/s, %1.2f nodes/hr", rate, rate * 3600)
            println "[$watch] Traversed #$traversalCount $id [$formattedRate]"
        }
    }

    private static FluentIterable<SimpleTree> getTreeTraversor(NodeState state){
        def traversor = new TreeTraverser<SimpleTree>(){
            Iterable<SimpleTree> children(SimpleTree root) {
                return root.children()
            }
        }
        return traversor.preOrderTraversal(new SimpleTree(state, "/"))
    }

    private static class SimpleTree {
        final NodeState state
        final String path

        SimpleTree(NodeState state, String path){
            this.state = state
            this.path = path
        }

        Iterable<SimpleTree> children(){
            return Iterables.transform(state.childNodeEntries, { ChildNodeEntry cne ->
                new SimpleTree(cne.nodeState, PathUtils.concat(path, cne.name))} as Function)
        }
    }
}

new NodeStoreTraversalRateEstimator(session.whiteboard, session.store).readAll()

