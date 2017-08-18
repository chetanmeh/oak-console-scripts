/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package felixconsole.indexImport

import groovy.io.FileType
import org.apache.jackrabbit.oak.api.CommitFailedException
import org.apache.jackrabbit.oak.api.PropertyState
import org.apache.jackrabbit.oak.api.Type
import org.apache.jackrabbit.oak.commons.PathUtils
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndex
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexEditorContext
import org.apache.jackrabbit.oak.spi.commit.EditorDiff
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor
import org.apache.jackrabbit.oak.spi.state.NodeBuilder
import org.apache.jackrabbit.oak.spi.state.NodeState
import org.apache.jackrabbit.oak.spi.state.NodeStore

import java.lang.reflect.Method

import static LOG.log
import static com.google.common.base.Preconditions.checkArgument
import static com.google.common.base.Preconditions.checkNotNull
import static XVersionUtils.configureUniqueId
import static XVersionUtils.createFSDirectory
import static XVersionUtils.createOakDirectory
import static XVersionUtils.getStore
import static XVersionUtils.luceneFileCopyContext
import static XVersionUtils.merge
import static XVersionUtils.startWhiteboardIndexEditorProvider
import static XVersionUtils.stopWhiteboardIndexEditorProvider
import static XVersionUtils.whiteboardIndexEditorProvider

class LOG {
    static def out
    static void log(def msg) {
        out.println(msg)
    }
}
LOG.out = out

class XVersionUtils {
    static Class loadCoreClass(String className) {
        return NodeStore.class.classLoader.loadClass(className)
    }

    static NodeStore getStore(def osgi) {
        osgi.getService(org.apache.sling.jcr.api.SlingRepository.class).manager.store
    }

    static def merge(NodeStore nodeStore, NodeBuilder rootBuilder) {
        def EMPTY = loadCoreClass('org.apache.jackrabbit.oak.spi.commit.CommitInfo').EMPTY
        def INSTANCE = loadCoreClass('org.apache.jackrabbit.oak.spi.commit.EmptyHook').INSTANCE
        nodeStore.merge(rootBuilder, INSTANCE, EMPTY)
    }

    static def getWhiteboardIndexEditorProvider() {
        Class WhiteboardIndexEditorProvider
        try {
            WhiteboardIndexEditorProvider =
                    loadCoreClass('org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardIndexEditorProvider')
        } catch (ignored) {
            WhiteboardIndexEditorProvider =
                    loadCoreClass('org.apache.jackrabbit.oak.plugins.index.WhiteboardIndexEditorProvider')
        }

        return WhiteboardIndexEditorProvider.newInstance()
    }

    static Class loadLuceneClass(String className) {
        return LuceneIndex.class.classLoader.loadClass(className)
    }

    static def createOakDirectory(NodeBuilder rootBuilder, NodeBuilder idxBuilder, String indexPath) {
        def OakDirectory = loadLuceneClass('org.apache.jackrabbit.oak.plugins.index.lucene.OakDirectory')

        def IndexDefinition
        try {
            IndexDefinition = loadLuceneClass('org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition')
        } catch (ignored) {
        }

        def oakDir

        /**
         * The order of 'trying' {@code OakDirectory} constructors is very relevant here. The first one without
         * {@code IndexDefinition} is the oldest incarnate and the only one which just works with {@code NodeBuider}
         * for :data node. Current code have a constructor which accepts {@code NodeBuilder} for index definition node
         * along with name (String) of data node (or suggest-data, etc). There is another constructor in current code
         * which defaults the name to {@code :data}. Unfortunately, that one breaks the earlier version which required
         * the NodeBuider to be pointing to :data directly. Hence we try explitly name :data first and then fall down.
         **/
        if (IndexDefinition == null) {
            oakDir = OakDirectory.newInstance(idxBuilder.child(':data'))
        } else {
            def idxDefn
            try {
                idxDefn = IndexDefinition.newInstance(rootBuilder.getNodeState(), idxBuilder.getNodeState())
            } catch (ignored) {}

            if (idxDefn == null) {
                idxDefn = IndexDefinition.newInstance(rootBuilder.getNodeState(), idxBuilder.getNodeState(), indexPath)
            }
            checkNotNull(idxDefn, "Couldn't instantiate IndexDefinition")

            try {
                oakDir = OakDirectory.newInstance(idxBuilder, ':data', idxDefn, false)
            } catch (ignored) {}

            if (oakDir == null) {
                NodeBuilder dataBuilder = idxBuilder.child(':data')
                try {
                    oakDir = OakDirectory.newInstance(dataBuilder, idxDefn, false)
                } catch (ignored) {}

                if (oakDir == null) {
                    oakDir = OakDirectory.newInstance(dataBuilder, idxDefn)
                }
            }
        }

        checkNotNull(oakDir, "Couldn't instantiate OakDirectory")
        return oakDir
    }

    static void startWhiteboardIndexEditorProvider(def whiteboardIndexEditorProvider, def bundleContext) {
        whiteboardIndexEditorProvider.start(new OsgiWhiteboard(bundleContext))
    }

    static void stopWhiteboardIndexEditorProvider(def whiteboardIndexEditorProvider) {
        whiteboardIndexEditorProvider.stop()
    }

    static def createFSDirectory(File fsDirFile) {
        Class FSDirectory = loadLuceneClass('org.apache.lucene.store.FSDirectory')
        return FSDirectory.getMethod('open', File.class).invoke(null, fsDirFile)
    }

    static def getLuceneFileCopyContext() {
        Class IOContext = loadLuceneClass('org.apache.lucene.store.IOContext')
        return IOContext.getField('DEFAULT').get(null)
    }

    static String configureUniqueId(NodeBuilder indexDefinition) {
        String uid = null
        Method setupUniqueId
        try {
            setupUniqueId = LuceneIndexEditorContext.getMethod('configureUniqueId', NodeBuilder.class)

            uid = setupUniqueId.invoke(null, indexDefinition)
        } catch (ignored) {
            //
        }

        return uid
    }
}

class ExportedIndexParser {
    File indexPath
    def checkpoint
    Map<String, File> indexedDirsInfo = [:]

    ExportedIndexParser(String indexPath) {
        this.indexPath = new File(indexPath)
        parse()
    }

    void parse() {
        readStoredCheckpoint()
        readIndicesData()
    }

    void readStoredCheckpoint() {
        Properties properties = new Properties()
        File propertiesFile = new File(indexPath, 'indexer-info.properties')
        checkArgument(propertiesFile.exists(),
                "Exported index metadata file ${propertiesFile} doesn't exist")
        propertiesFile.withInputStream {
            properties.load(it)
        }
        checkpoint = properties.getProperty("checkpoint")

        log ("Using stored checkpoint: ${checkpoint}")
    }

    void readIndicesData() {
        indexedDirsInfo = [:]
        indexPath.eachFile(FileType.DIRECTORIES) { indexDir ->
            def indexInfo = new Properties()
            File indexInfoFile = new File(indexDir, 'index-details.txt')
            indexInfoFile.withInputStream {
                indexInfo.load(it)
            }

            String indexPath = indexInfo.get('indexPath')
            for (String propName in indexInfo.propertyNames()) {
                if (propName.startsWith('dir.data')) { //only import data dir
                    def indexDirLocalName = propName.substring('dir.'.length())
                    indexedDirsInfo[indexPath] = new File(indexDir, indexDirLocalName)
                }
            }
        }

        log ("Following indices would be imported from corresponding paths: ${indexedDirsInfo}")
    }
}

class LaneManager {
    Set<String> indices
    NodeState root

    String tempLaneName = 'async-oob-reindex'

    Map<String, PropertyState> laneMappings = [:]

    LaneManager(Set<String> indices, NodeState root) {
        this.indices = indices
        this.root = root

        collectLaneMappings()
    }

    void collectLaneMappings() {
        for (indexPath in indices) {
            laneMappings[indexPath] = getIndexLane(root, indexPath)
        }

        log ("Collected original lanes: ${laneMappings}")
    }

    void changeLanes(NodeBuilder rootBuilder) {
        for (indexPath in indices) {
            def isArray = laneMappings.get(indexPath).isArray()
            def value = isArray ? Collections.singletonList(tempLaneName) : tempLaneName
            def type = isArray ? Type.STRINGS : Type.STRING
            getIndexNodeBuilder(rootBuilder, indexPath).setProperty("async", value, type)
        }

        log ("Switched to temp lane: ${tempLaneName}")
    }

    void revertOriginalLanes(NodeBuilder rootBuilder) {
        for (indexPath in indices) {
            getIndexNodeBuilder(rootBuilder, indexPath)
                    .setProperty(laneMappings.get(indexPath))
        }

        log ('Reverted original lanes')
    }

    static NodeBuilder getIndexNodeBuilder(NodeBuilder root, String indexPath) {
        NodeBuilder indexNodeBuilder = root
        for (String elem : PathUtils.elements(indexPath)) {
            indexNodeBuilder = indexNodeBuilder.getChildNode(elem)
        }
        return indexNodeBuilder
    }

    static NodeState getIndexNodeState(NodeState root, String indexPath) {
        NodeState indexNodeState = root
        for (String elem : PathUtils.elements(indexPath)) {
            indexNodeState = indexNodeState.getChildNode(elem)
        }
        return indexNodeState
    }

    static PropertyState getIndexLane(NodeState root, String indexPath) {
        return getIndexNodeState(root, indexPath).getProperty("async")
    }
}

class IndexImporter {
    def osgi
    def bundleContext

    NodeStore nodeStore

    LaneManager laneManager

    NodeBuilder rootBuilder

    Map<String, File> indexedDirsInfo
    String heldBeforeCheckpoint
    String indexPath


    IndexImporter(def osgi, def bundleContext, String indexPath) {
        this(osgi, bundleContext, indexPath, new ExportedIndexParser(indexPath))
    }

    IndexImporter(def osgi, def bundleContext, String indexPath, ExportedIndexParser exportedIndexParser) {
        this.osgi = osgi
        this.bundleContext = bundleContext

        nodeStore = getStore(osgi)

        this.indexPath = indexPath
        indexedDirsInfo = exportedIndexParser.indexedDirsInfo
        this.heldBeforeCheckpoint = exportedIndexParser.checkpoint

        NodeState root = nodeStore.getRoot()

        laneManager = new LaneManager(indexedDirsInfo.keySet(), root)

        rootBuilder = root.builder()
    }

    def verifyInputs() {
        def beforeCP = nodeStore.retrieve(heldBeforeCheckpoint)
        checkNotNull(beforeCP, "Couldn't retrieve before checkpoint - ${heldBeforeCheckpoint}")
    }

    def switchLanes() {
        laneManager.changeLanes(rootBuilder)
    }

    def copyAllIndices() {
        indexedDirsInfo.each { indexedDir ->
            copyIndex(indexedDir.key, indexedDir.value)
        }
    }

    def copyIndex(String indexPath, File fsDirFile) {

        def idxBuilder = rootBuilder
        for (def elem : PathUtils.elements(indexPath)) {
            idxBuilder = idxBuilder.getChildNode(elem)
        }

        //remove :data and :status
        idxBuilder.getChildNode(":data").remove()
        idxBuilder.getChildNode(":status").remove()

        //increment reindexCount if exists
        PropertyState reindexCount = idxBuilder.getProperty('reindexCount')
        if (reindexCount != null) {
            idxBuilder.setProperty('reindexCount', reindexCount.getValue(Type.LONG) + 1)
        }

        setupUniqueId(idxBuilder)

        def oakDir, fsDir
        try {
            oakDir = createOakDirectory(rootBuilder, idxBuilder, indexPath)
            checkNotNull(oakDir, 'Could not create OakDirectory instance')

            fsDir = createFSDirectory(fsDirFile)

            for (String file : fsDir.listAll()) {
                fsDir.copy(oakDir, file, file, luceneFileCopyContext)
            }
        } finally {
            if (oakDir != null) {
                oakDir.close()
            }
            if (fsDir != null) {
                fsDir.close()
            }
        }

        log ("Imported fsIndex (${fsDirFile}) into repo path (${indexPath})")
    }

    static setupUniqueId(NodeBuilder indexDefinition) {
        String uid = configureUniqueId(indexDefinition)
        if (uid != null) {
            log("Created unique id (${uid}) on ${indexDefinition}")
        } else {
            log("Unable to create uniqe id")
        }
    }

    def indexCatchup() {
        String cpBefore = heldBeforeCheckpoint
        String cpAfter = nodeStore.getRoot().getChildNode(':async').getString('async')

        NodeState before = nodeStore.retrieve(cpBefore)
        NodeState after = nodeStore.retrieve(cpAfter)
        checkNotNull(after, "Couldn't retrieve after checkpoint - ${cpAfter}")

        IndexEditorProvider indexEditorProvider = (IndexEditorProvider)whiteboardIndexEditorProvider
        try {
            startWhiteboardIndexEditorProvider(indexEditorProvider, bundleContext)

            def iucNOOP = new IndexUpdateCallback() {
                void indexUpdate() {}
            }
            IndexUpdate indexUpdate = new IndexUpdate(
                    indexEditorProvider,
                    laneManager.tempLaneName,
                    rootBuilder.getNodeState(),
                    rootBuilder,
                    iucNOOP
            )

            CommitFailedException exception = EditorDiff.process(VisibleEditor.wrap(indexUpdate), before, after)

            if (exception != null) {
                throw exception
            }
        } finally {
            if (indexEditorProvider != null) {
                stopWhiteboardIndexEditorProvider(indexEditorProvider)
            }
        }

        log ("Caught up indices from ${cpBefore} to ${cpAfter}")
    }

    def revertLanes() {
        laneManager.revertOriginalLanes(rootBuilder)
    }

    def merge() {
        merge(nodeStore, rootBuilder)

        rootBuilder = nodeStore.getRoot().builder()
    }

    def doAll() {
        verifyInputs()

        switchLanes()

        copyAllIndices()

        indexCatchup()

        revertLanes()

        merge()
    }
}

def indexPath = '/path/to/generated/data/by/oak-run/indexing-result/indexes'

new IndexImporter(osgi, bundleContext, indexPath).doAll()