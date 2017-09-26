import com.google.common.base.Stopwatch
import groovy.transform.CompileStatic
import org.apache.jackrabbit.oak.api.CommitFailedException
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback
import org.apache.jackrabbit.oak.plugins.index.NodeTraversalCallback
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter
import org.apache.jackrabbit.oak.spi.commit.DefaultEditor
import org.apache.jackrabbit.oak.spi.commit.Editor
import org.apache.jackrabbit.oak.spi.commit.EditorDiff
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor
import org.apache.jackrabbit.oak.spi.state.NodeState
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils
import org.apache.jackrabbit.oak.spi.state.NodeStore
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE

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

@CompileStatic
class TraversingEditorEstimator {
    final NodeStore store
    String path = System.getProperty("traversalPath", "/")

    TraversingEditorEstimator(Whiteboard wb, NodeStore ns){
        this.store = ns
    }

    def readAll(){
        IndexingProgressReporter ipr =
                new IndexingProgressReporter(IndexUpdateCallback.NOOP, NodeTraversalCallback.NOOP)
        TraversingEditor mainEditor = new TraversingEditor()
        Editor editor = VisibleEditor.wrap(ipr.wrapProgress(mainEditor))

        println("Traversing from path $path")
        NodeState pathState = NodeStateUtils.getNode(store.root, path)

        def w = Stopwatch.createStarted()
        ipr.reindexingTraversalStart(path)
        EditorDiff.process(editor, MISSING_NODE, pathState)
        ipr.reindexingTraversalEnd()
        w.stop()

        println("Reindexing Traversal finished in $w")
        println("Collected hash ${mainEditor.hashCodeCollector}")
    }

    static class TraversingEditor extends DefaultEditor {
        int hashCodeCollector = 0

        @Override
        void enter(NodeState before, NodeState after) throws CommitFailedException {
            hashCodeCollector += after.toString().hashCode()
            if (after.hasChildNode("jcr:content")) {
                hashCodeCollector += after.getChildNode("jcr:content").toString().hashCode()
            }
        }

        @Override
        Editor childNodeAdded(String name, NodeState after)
                throws CommitFailedException {
            return this
        }

        @Override
        Editor childNodeChanged(String name,
                                       NodeState before,
                                       NodeState after)
                throws CommitFailedException {
            return this
        }

        @Override
        Editor childNodeDeleted(String name, NodeState before)
                throws CommitFailedException {
            return this
        }
    }
}

new TraversingEditorEstimator(session.whiteboard, session.store).readAll()

