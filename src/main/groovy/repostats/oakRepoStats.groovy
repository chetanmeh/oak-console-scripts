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


import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry
import com.google.common.base.Predicate
import com.google.common.base.Stopwatch
import com.google.common.collect.AbstractIterator
import com.google.common.collect.FluentIterable
import com.google.common.collect.Iterators
import com.google.common.collect.PeekingIterator
import com.google.common.collect.Sets
import com.google.common.collect.TreeTraverser
import groovy.json.JsonOutput
import groovy.text.SimpleTemplateEngine
import groovy.transform.*
import org.apache.commons.io.output.WriterOutputStream
import org.apache.jackrabbit.JcrConstants
import org.apache.jackrabbit.oak.api.Blob
import org.apache.jackrabbit.oak.api.PropertyState
import org.apache.jackrabbit.oak.api.Tree
import org.apache.jackrabbit.oak.api.Type
import org.apache.jackrabbit.oak.commons.PathUtils
import org.apache.jackrabbit.oak.commons.sort.StringSort
import org.apache.jackrabbit.oak.plugins.blob.datastore.InMemoryDataRecord
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory
import org.apache.jackrabbit.oak.spi.state.NodeStore

import javax.jcr.PropertyType
import java.util.concurrent.TimeUnit

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount

@CompileStatic
class RepoStats {
    final boolean excludeIndexStats = Boolean.getBoolean("excludeIndexStats")
    final String rootPath = System.getProperty("rootPath", "/")

    final static String VERSION = "1.2"

    MetricRegistry metrics = new MetricRegistry()
    NodeStore nodeStore;
    List<String> excludedIndexes =[]
    BinaryStats binaryStats = new BinaryStats()

    long indexNodeCount;
    long systemNodeCount;
    long versionStoreCount;
    long permStoreCount;
    long jcrNodeCount;
    long totalCount

    boolean statsWritten

    Map<String, NodeTypeStats> nodeTypeStats = [:].withDefault {key -> new NodeTypeStats(name:(String)key)}
    Map<String, MimeTypeStats> mimeTypeStats = [:].withDefault { key -> new MimeTypeStats(name:(String)key)}
    Map<String, PathStats> pathStats = [:].withDefault { key -> new PathStats(path:(String)key)}

    List<IndexStats> propertyIndexStats = []
    List<LuceneIndexStats> luceneIndexStats = []
    final int maxIndexedEntryCount = 10

    def detailedStatsEnabled = true
    private Set<String> detailedStats = [
            'dam:AssetContent',
            'cq:PageContent',
            'rep:User',
            'rep:Group',
            'nt:file',
            'nt:versionHistory'
    ] as HashSet

    private List<String> impPaths = [
            '/content/dam',
            '/var/audit'
    ]

    private File statsFileJson = new File("repo-stats.json")
    private File statsFileTxt = new File("repo-stats.txt")
    Stopwatch watch = Stopwatch.createStarted()
    boolean useChildNodeCount
    private final Predicate<Tree> countingPredicate =  new Predicate<Tree>(){
            boolean apply(Tree input) {
                if (input && input.exists()) {
                    tick(input)
                    collectTypeStats(input)
                    collectBinaryStats(input)
                }
                return true
            }
    }

    def dumpStats(){
        init()
        Tree root = TreeFactory.createReadOnlyTree(nodeStore.root)
        root = getTree(root, rootPath)
        getFilteringTraversor(root).each { Tree t ->
            def path = t.path
            if (path.endsWith('oak:index')){
                if (!excludeIndexStats) {
                    long count = collectIndexStats(t)
                    if (path == '/oak:index') {
                        pathStats[path].count += count
                    }
                }
            } else if (path.startsWith('/jcr:system')){
                systemNodeCount++
                if (path.startsWith('/jcr:system/jcr:versionStorage')){
                    versionStoreCount++
                } else if (path.startsWith('/jcr:system/rep:permissionStore')){
                    permStoreCount++
                }
            } else {
                if (t.hasProperty(JCR_PRIMARYTYPE)) {
                    jcrNodeCount++
                }
            }
            collectPathStats(t.path)
        }

        writeStats()
        statsWritten = true
    }

    Tree getTree(Tree tree, String path) {
        for (String element : PathUtils.elements(path)) {
            if (PathUtils.denotesParent(element)) {
                if (tree.isRoot()) {
                    return null
                } else {
                    tree = tree.getParent()
                }
            } else if (!PathUtils.denotesCurrent(element)) {
                tree = tree.getChild(element)
            }  // else . -> skip to next element
        }
        return tree
    }

    def init() {
        useChildNodeCount = nodeStore.root.class.name.endsWith("SegmentNodeState")
        if (useChildNodeCount){
            println("Using useChildNodeCount for estimating immediate children count")
        }

        if (excludeIndexStats){
            println "Index stats collection excluded via flag 'excludeIndexStats'"
        }

        println "Using root path as ${rootPath}"

        addShutdownHook {
            if (!statsWritten){
                writeStats()
            }
        }
    }

    //~----------------------------------------< PathStats >

    @EqualsAndHashCode
    @Sortable
    static class PathStats{
        long count
        String path
        String jcrPercent
        String overallPercent

        String getPathStr(){
            if (!path.startsWith('/')){
                return '/' + path
            }
            return path
        }

        String getCountStr(){
            return displayCount(count)
        }

        void computePercent(long jcrCount, long overAllCount){
            jcrPercent = String.format("%1.2f %%", (double)count/jcrCount * 100)
            overallPercent = String.format("%1.2f %%", (double)count/overAllCount * 100)

            if (path.contains('oak:index') || path.contains('jcr:system')){
                jcrPercent = "-"
            }
        }
    }

    def collectPathStats(String path) {
        if (PathUtils.denotesRoot(path)){
            return
        }

        String firstPart = PathUtils.elements(path).iterator().next()
        pathStats[firstPart].count++

        impPaths.each {p ->
            if (path.startsWith(p)){
                pathStats[p].count++
            }
        }
    }

    def processPathStats() {
        pathStats.values().each {PathStats ps -> ps.computePercent(jcrNodeCount, totalCount)}
    }

    //~----------------------------------------< Node Statistics >

    static class NodeTypeStats{
        String name
        long count
        long subTreeCount
        long referredBinarySize
        int percentJcrContent
    }

    def collectTypeStats(Tree t) {
        PropertyState primaryType = t.getProperty(JCR_PRIMARYTYPE)
        if (primaryType) {
            String type = primaryType.getValue(Type.NAME)
            nodeTypeStats[type].count++

            if (detailedStatsEnabled && detailedStats.contains(type)) {
                collectSubtreeStats(nodeTypeStats[type], t)
            }
        }

        PropertyState mixinType = t.getProperty(JCR_MIXINTYPES)
        if (mixinType){
            mixinType.getValue(Type.NAMES).each {type ->
                nodeTypeStats[type].count++
            }
        }
    }

    def collectSubtreeStats(NodeTypeStats stats, Tree tree) {
        //Get a histogram of number of child nodes
        FluentIterable<Tree> itr = getTreeTraversor(tree)
        int rootDepth = PathUtils.getDepth(tree.path)
        itr = itr.filter(excludeTreeOfSameType(stats, tree))
        int depth = 0
        itr = itr.filter({Tree t -> depth = Math.max(depth, PathUtils.getDepth(t.path) - rootDepth); return true} as Predicate)
        int subTreeCount
        switch (stats.name) {
            case 'dam:AssetContent' :
                subTreeCount = collectAssetStats(stats, tree, itr);
                break
            default:
                subTreeCount = itr.size()
        }

        stats.subTreeCount += subTreeCount
        metrics.histogram(metricName(stats.name, "subtree_depth")).update(depth)
        metrics.histogram(metricName(stats.name, "subtree_count")).update(subTreeCount)
    }

    int collectAssetStats(NodeTypeStats stats, Tree tree, FluentIterable<Tree> itr) {
        long originalSize = 0
        long renditionSize = 0
        long xmpCount = 0
        itr = itr.filter({Tree t ->
            if (t.name == 'jcr:content') {
                MimeTypeStats binStats = getBinaryStats(t)
                if (binStats) {
                    if (t.getParent().name == 'original') {
                        originalSize += binStats.totalSize
                    } else {
                        renditionSize += binStats.totalSize
                        metrics.histogram(metricName(stats.name, "renditions")).update(binStats.totalSize)
                    }
                }
            }
            if (t.name == 'metadata'){
                xmpCount = getTreeTraversor(t).size()
            }
            return true
        } as Predicate<Tree>)

        //Read the iterator
        int subtreeCount = itr.size()

        metrics.histogram(metricName(stats.name, "original")).update(originalSize)
        metrics.histogram(metricName(stats.name, "renditions_total")).update(renditionSize)
        metrics.histogram(metricName(stats.name, "xmp")).update(xmpCount)

        stats.referredBinarySize += originalSize + renditionSize

        return subtreeCount
    }

    private Predicate<Tree> excludeTreeOfSameType(NodeTypeStats stats, Tree tree) {
        new Predicate<Tree>() {
            boolean apply(Tree input) {
                PropertyState primaryType = input.getProperty(JCR_PRIMARYTYPE)
                //Exclude any sub nodetree which is based on current type. Like a dam:AssetContent
                //having dam:AssetContent as one of the sub nodes
                if (primaryType
                        && primaryType.getValue(Type.NAME) == stats.name
                        && input.path != tree.path) {
                    return false
                }
                return true
            }
        }
    }

    static String metricName(String nodeType, String type){
        return "${type}_${nodeType}"
    }

    private List<NodeTypeStats> processNodeTypeStats() {
        List<NodeTypeStats> nodeTypeStatsList = new ArrayList(nodeTypeStats.values())
        nodeTypeStatsList.sort { -it.count }
        nodeTypeStatsList.each {NodeTypeStats s ->
            s.percentJcrContent = (int) ((double) s.subTreeCount/jcrNodeCount * 100)
        }
        return nodeTypeStatsList
    }

    def tick(Tree t) {
        if (++totalCount % 10000 == 0){
            println("Traversed $totalCount (${withSuffix(totalCount)})...")
        }

        if (t.path.contains('/oak:index')){
            indexNodeCount++
        }
    }

    //~-----------------------------------------------------< Binary Statistics >

    static final String MIMETYPE_UNKNOWN = 'unknown'

    @ToString(excludes = ['blobIds'], includeNames = true)
    @EqualsAndHashCode
    @Sortable(excludes = ['blobIds'])
    static class MimeTypeStats{
        long externalSize
        long inlinedSize
        int count
        String name
        Set<String> blobIds = Collections.emptySet()

        String getSizeStr(){
            //Used in template writeBinaryStats
            return "${withSizeSuffix(totalSize)} (E:${withSizeSuffix(externalSize)}, I:${withSizeSuffix(inlinedSize)})"
        }

        long getTotalSize(){
            return externalSize + inlinedSize
        }

        MimeTypeStats add(MimeTypeStats stats){
            if (stats) {
                count += stats.count
                externalSize += stats.externalSize
                inlinedSize += stats.inlinedSize
            }
            return this
        }
    }

    static class BinaryStats {
        TypedBinaryStats files = new TypedBinaryStats()
        TypedBinaryStats versionedFiles = new TypedBinaryStats()
        TypedBinaryStats index = new TypedBinaryStats()
        TypedBinaryStats unknown = new TypedBinaryStats()
        TypedBinaryStats all = new TypedBinaryStats()

        void add(Tree t, long size, int count, Set<String> blobIds, long inlineSize){
            if (PathUtils.denotesRoot(t.path)){
                return
            }

            TypedBinaryStats typedStats = unknown

            if (t.parent.getProperty(JCR_PRIMARYTYPE)?.getValue(Type.STRING) == 'nt:file'){
                typedStats = files
            } else if (t.path.contains('/oak:index')){
                typedStats = index
            } else if (t.path.startsWith('/jcr:system')
                    && t.parent.getProperty('jcr:frozenPrimaryType')?.getValue(Type.STRING) == 'nt:file'){
                typedStats = versionedFiles
            }

            typedStats.add(size, inlineSize, count, blobIds)
            all.add(size, inlineSize, count, blobIds)
        }

        void computeSize(){
            List<TypedBinaryStats> allStats = [files, versionedFiles, index, unknown, all]
            allStats.each {it.computeSize(); }
            versionedFiles.diff(files)
            unknown.diff(files)

            allStats.each {it.close()}
        }
    }

    static class TypedBinaryStats{
        long inlineSize
        long externalSize
        long actualSize = -1
        int count
        StringSort blobIds = new StringSort(1000, String.CASE_INSENSITIVE_ORDER)

        String getSizeStr(){
            //Used in template writeBinaryStats
            return humanReadableByteCount(externalSize)
        }

        String getActualSizeStr(){
            //Used in template writeBinaryStats
            return humanReadableByteCount(actualSize)
        }

        String getBlobIds() {
            return "NA"
        }

        String toString(){
            "${withSizeSuffix(totalSize)} ($count) (Deduplicated ${withSizeSuffix(actualSize)}, Inlined ${withSuffix(inlineSize)})"
        }

        long getTotalSize(){
            return inlineSize + externalSize
        }

        void add(long size, long inlineSize, int count, Set<String> blobIds){
            this.externalSize += size
            this.count += count
            this.inlineSize += inlineSize
            def that = this
            blobIds.each {id -> that.addId(id)}
        }

        private void addId(String id){
            if (!isInlined(id)) {
                this.@blobIds.add(id)
            }
        }

        void computeSize(){
            if (actualSize < 0){
                actualSize = computeSizeFromSort(blobIds())
            }
        }

        void close(){
            blobIds().close()
        }

        StringSort blobIds(){
            this.@blobIds
        }

        void diff(TypedBinaryStats other) {
            //Computes the diff like this - other i.e. size of blobs which are present
            //in this but not present in other
            StringSort diffIds = new StringSort(1000, String.CASE_INSENSITIVE_ORDER)
            Iterator<String> diffItr = new DiffIterator(blobIds(), other.blobIds())
            while(diffItr.hasNext()){
                diffIds.add(diffItr.next())
            }
            actualSize = computeSizeFromSort(diffIds)
            diffIds.close()
        }

        private long computeSizeFromSort(StringSort blobIds) {
            long totalSize = 0
            blobIds.sort()
            blobIds.each { id ->
                totalSize += RepoStats.getBlobSizeFromId(id)
            }
            return totalSize
        }
    }

    def collectBinaryStats(Tree tree) {
        MimeTypeStats stats = getBinaryStats(tree)
        if (stats) {
            mimeTypeStats[stats.name].add(stats)
            binaryStats.add(tree, stats.externalSize, stats.count, stats.blobIds, stats.inlinedSize)

            metrics.histogram("inlined_binary").update(stats.inlinedSize)
        }
    }

    MimeTypeStats getBinaryStats(Tree tree){
        long size = 0
        long inlinedSize = 0
        Set<String> blobIds = Sets.newHashSet()

        def blobProcessor = {Blob b->
            long blobSize = getBlobSize(b)

            if (b.getContentIdentity() != null){
                blobIds.add(b.getContentIdentity())
            }

            if (isInlined(tree, b)){
                inlinedSize += blobSize
            } else {
                size += blobSize
            }
        }

        getProps(tree).each {PropertyState ps ->
            if (ps.type.tag() != PropertyType.BINARY){
                return
            }
            if (ps.isArray()){
                for (int i = 0; i < ps.count(); i++) {
                    Blob b = ps.getValue(Type.BINARY, i)
                    blobProcessor(b)
                }
            } else {
                Blob b = ps.getValue(Type.BINARY)
                blobProcessor(b)
            }
        }

        if (size || inlinedSize){
             String mimeType = tree.getProperty(JcrConstants.JCR_MIMETYPE)?.getValue(Type.STRING)
            if (!mimeType){
                mimeType = MIMETYPE_UNKNOWN
            }
            return new MimeTypeStats(name: mimeType, count: 1, externalSize: size,
                    blobIds: blobIds, inlinedSize: inlinedSize)
        }
        return null
    }

    def computeBlobStats() {
        binaryStats.computeSize()

    }

    static boolean isInlined(Tree t, Blob blob){
        String blobId = blob.contentIdentity
        return isInlined(blobId)
    }

    static boolean isInlined(String blobId) {
        if (!blobId) {
            return true
        }

        if (InMemoryDataRecord.isInstance(blobId)) {
            return true
        }

        //OAK-4789 - SegmentBlob returns recordId. Check if it contains '.'
        //RecordId indicates its inlined blob
        if (blobId.contains(".")) {
            return true
        }

        return false
    }

    static long getBlobSize(Blob blob){
        long size = 0
        try{
            //Check first directly
            size = blob.length()
        } catch (Exception e){
            //May be blobstore not configured. Extract length encoded in id
            String blobId = blob.getContentIdentity()
            size = getBlobSizeFromId(blobId)
        }
        return size
    }

    static long getBlobSizeFromId(String blobId) {
        if (blobId) {
            int indexOfHash = blobId.lastIndexOf('#')
            if (indexOfHash > 0) {
               return blobId.substring(indexOfHash + 1) as long
            }
        }
        return 0
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    static Iterable<PropertyState> getProps(Tree t){
        //Workaround Groovy compiler issue
        return t.properties
    }

    //~-----------------------------------------------------< Index Statistics >

    @ToString(excludes = ['indexedStats'], includeNames = true)
    static class IndexStats {
        String name
        boolean disabled
        boolean unique
        long childCount
        long entryCount
        long indexedEntryCount
        Set<IndexEntryStats> indexedStats = new HashSet<>()
    }

    @EqualsAndHashCode
    @Sortable
    static class IndexEntryStats {
        long entryCount
        long childCount
        String name
    }

    @EqualsAndHashCode
    @Sortable
    static class LuceneIndexStats {
        long size
        int count
        String name

        String getSizeStr(){
            //Used in template writeBinaryStats
            return humanReadableByteCount(size)
        }
    }

    long collectIndexStats(Tree oakIndex){
        long indexNodeCountAtStart = indexNodeCount
        oakIndex.children.each {Tree indexNode ->

            def idxName = indexNode.path

            if (excludedIndexes.contains(idxName)){
                println("Excluding index $idxName")
                return
            }

            def type = indexNode.getProperty("type")?.getValue(Type.STRING)

            if (type == 'property' || type == 'disabled'){
                println("Processing $idxName")

                countingPredicate.apply(indexNode)

                boolean unique = indexNode.getProperty('unique')?.getValue(Type.BOOLEAN)
                def idxStats = new IndexStats(name:idxName, unique: unique, disabled: type == 'disabled')

                def contentNode = indexNode.getChild(':index')
                countingPredicate.apply(contentNode)

                if (unique) {
                    collectUniqueIndexStats(contentNode, idxStats)
                } else {
                    collectPropertyIndexStats(contentNode, idxStats)
                }

                println("  $idxStats")
                propertyIndexStats << idxStats
            } else if (type == 'lucene'){
                collectLuceneIndexStats(idxName, indexNode)
            } else {
                indexNodeCount += getCountingTraversor(indexNode).size()
            }
        }
        return indexNodeCount - indexNodeCountAtStart
    }

    private void collectLuceneIndexStats(String idxName, Tree indexNode) {
        LuceneIndexStats idxStats = new LuceneIndexStats(name: idxName)
        indexNodeCount += getCountingTraversor(indexNode)
                .filter({ Tree idxTree ->
                    MimeTypeStats mimeStats = getBinaryStats(idxTree)
                    if (mimeStats) {
                        idxStats.size += mimeStats.externalSize
                        idxStats.count += mimeStats.count
                    }
                    return true
                } as Predicate)
                .size()
        luceneIndexStats << idxStats
    }

    private void collectPropertyIndexStats(Tree contentNode, IndexStats idxStats) {
        TreeSet<IndexEntryStats> indexedStats = new TreeSet<>(Collections.reverseOrder())
        contentNode.children.each { Tree indexedStateEntry ->
            IndexEntryStats indexEntryStats = getIndexEntryStats(indexedStateEntry)
            idxStats.entryCount += indexEntryStats.entryCount
            idxStats.childCount += indexEntryStats.childCount

            idxStats.indexedEntryCount++
            indexedStats << indexEntryStats

            if (indexedStats.size() > maxIndexedEntryCount){
                indexedStats.pollLast()
            }
        }

        idxStats.indexedStats = indexedStats
    }

    private void collectUniqueIndexStats(Tree contentNode, IndexStats idxStats) {
        if (useChildNodeCount) {
            //use faster route for big hierarchies
            long count = contentNode.getChildrenCount(Integer.MAX_VALUE);
            idxStats.childCount = count
            totalCount += count
            indexNodeCount += count
            println("Obtained count [$count] via getChildrenCount")
        } else {
            idxStats.childCount = getCountingTraversor(contentNode).size()
        }

        idxStats.entryCount = idxStats.childCount
    }

    private IndexEntryStats getIndexEntryStats(Tree indexEntry){
        FluentIterable<Tree> itr = getCountingTraversor(indexEntry)
        def stats = new IndexEntryStats(name: indexEntry.name)
        itr.each {Tree ns  ->
            if (ns.hasProperty('match')){
                stats.entryCount++
            }
            stats.childCount++
        }
        return stats
    }

    //~--------------------------------------------< Stats Reporting >

    private void writeStats() {
        processPathStats()
        List<NodeTypeStats> nodeTypeStatsList = processNodeTypeStats()
        luceneIndexStats = luceneIndexStats.sort().reverse()

        propertyIndexStats.sort{-it.childCount}

        computeBlobStats()

        writeStatsInText(nodeTypeStatsList)
        writeStatsInJson(nodeTypeStatsList)

        println("Stats in json format dumped to ${statsFileJson.getAbsolutePath()}")
        println("Stats in tx format dumped to ${statsFileTxt.getAbsolutePath()}")
        println("Total time taken : $watch")
    }



    private void writeStatsInText(nodeTypeStats) {
        statsFileTxt.withPrintWriter { pw ->
            pw.println("Number of NodeStates")
            pw.println("\tTotal     : ${displayCount(totalCount)}")
            pw.println("\tin index  : ${displayCount(indexNodeCount)}")
            pw.println("\tin system : ${displayCount(systemNodeCount)}")
            pw.println("\tin JCR    : ${displayCount(jcrNodeCount)}")
            pw.println()
            pw.println("Number of NodeStates under /jcr:system")
            pw.println("\tTotal                                : ${displayCount(systemNodeCount)}")
            pw.println("\tin /jcr:system/jcr:versionStorage    : ${displayCount(versionStoreCount)}")
            pw.println("\tin /jcr:system/rep:permissionStore   : ${displayCount(permStoreCount)}")
            pw.println()
            pw.println("Binary stats")
            pw.println("The size reported below does not account for deduplication. Hence reported")
            pw.println("total size might be much more than actual datastore size")
            pw.println("\tTotal                                : ${binaryStats.all}")
            pw.println("\t\t nt:file                           : ${binaryStats.files}")
            pw.println("\t\t oak:index                         : ${binaryStats.index}")
            pw.println("\t\t versioned nt:file                 : ${binaryStats.versionedFiles}")
            pw.println("\t\t unknown                           : ${binaryStats.unknown}")
            pw.printf("Report generated on :%tc%n", new Date())

            pw.println()
            writeNodeTypeStats(pw, nodeTypeStats)
            writePathStats(pw)
            writeBinaryStats(pw)
            writeMetrics(pw)

            writeIndexStats(pw)
            pw.println("Total time taken   : $watch")
        }
    }


    private static String displayCount(long count){
        return "$count (${withSuffix(count)})"
    }

    private void writeStatsInJson(List<NodeTypeStats> nodeTypeStats) {
        def jsonData = [
                scriptVersion     : VERSION,
                totalCount        : totalCount,
                indexNodeCount    : indexNodeCount,
                systemNodeCount   : systemNodeCount,
                jcrNodeCount      : jcrNodeCount,
                versionStoreCount : versionStoreCount,
                permStoreCount    : permStoreCount,
                date              : String.format("%tc", new Date()),
                timestamp         : System.currentTimeMillis(),
                timetaken         : watch.elapsed(TimeUnit.SECONDS),
                timetakenStr      : watch.toString(),
                nodeTypeStats     : nodeTypeStats,
                pathStats         : pathStats,
                propertyIndexStats: propertyIndexStats,
                luceneIndexStats  : luceneIndexStats,
                binaryStats       : binaryStats,
                metrics           : getMetricsAsMap()
        ]

        statsFileJson.text = JsonOutput.prettyPrint(JsonOutput.toJson(jsonData))
    }

    private def getMetricsAsMap() {
        metrics.histograms.collectEntries {name, hist ->
            def histMap = [
                    count : hist.count,
                    median: hist.snapshot.median,
                    min   : hist.snapshot.min,
                    max   : hist.snapshot.max,
                    mean  : hist.snapshot.mean,
                    stdDev: hist.snapshot.stdDev,
                    p75   : hist.snapshot.'75thPercentile',
                    p95   : hist.snapshot.'95thPercentile',
                    p98   : hist.snapshot.'98thPercentile',
                    p99   : hist.snapshot.'99thPercentile',
                    p999  : hist.snapshot.'999thPercentile',
            ]
            return [name, histMap]
        }
    }

    private void writeBinaryStats(PrintWriter pw) {
        def columns = [
                [name: "name", displayName: "Name", size: 45],
                [name: "count", displayName: "Count", size: 10],
                [name: "totalSize", displayName: "Size", size: 15],
                [name: "sizeStr", displayName: "Size", size: 45],
        ]
        List<MimeTypeStats> stats = new ArrayList<>(mimeTypeStats.values())
        stats.sort()
        stats = stats.reverse()
        pw.println(dumpStats("Binary stats", stats, columns))
    }

    def writePathStats(PrintWriter pw) {
        def columns = [
                [name: "pathStr", displayName: "Path", size: 25],
                [name: "countStr", displayName: "Count", size: 25],
                [name: "jcrPercent", displayName: "% JCR", size: 10],
                [name: "overallPercent", displayName: "% All", size: 10],
        ]
        List<PathStats> stats = new ArrayList<>(pathStats.values())
        stats = stats.sort().reverse()
        pw.println(dumpStats("Path stats (Total ${displayCount(totalCount)})", stats, columns))
    }

    private void writeNodeTypeStats(PrintWriter pw, def nodeTypeStats) {
        def columns = [
                [name: "name", displayName: "Name", size: 45],
                [name: "count", displayName: "Count", size: 10],
                [name: "subTreeCount", displayName: "Subtree Count", size: 15],
                [name: "percentJcrContent", displayName: "% JCR", size: 10],
        ]
        pw.println(dumpStats("Nodetype stats", nodeTypeStats, columns))
    }

    private void writeMetrics(PrintWriter pw) {
        if (!metrics.getMetrics().isEmpty()) {
            pw.println("Histogram of count of child nodes of mentioned types")
            PrintStream ps = new PrintStream(new WriterOutputStream(pw))
            ConsoleReporter.forRegistry(metrics).outputTo(ps).build().report()
            ps.flush()
        }
    }

    private void writeIndexStats(PrintWriter pw) {
        def header = "Overall index stats"
        def output = writeIndexStats(header, propertyIndexStats)
        pw.println(output)

        writeLuceneIndexStats(pw);

        propertyIndexStats.each { s ->
            if (s.childCount > 0 && !s.unique) {
                header = "Stats for [${s.name}]."
                if (s.indexedEntryCount > maxIndexedEntryCount) {
                    header += " Listing top ${s.indexedStats.size()} out of total ${s.indexedEntryCount}"
                }
                output = writeIndexStats(header, s.indexedStats)
                pw.println(output)
            }
        }
    }

    def writeLuceneIndexStats(PrintWriter pw) {
        def columns = [
                [name:"name",displayName:"Name",size:45],
                [name:"size",displayName:"Size",size:10],
                [name:"sizeStr",displayName:"Size",size:10],
        ]
        pw.println(dumpStats("Lucene Index Stats", luceneIndexStats, columns))
    }

    private def writeIndexStats(String header, def stats) {
        def columns = [
                [name:"entryCount",displayName:"Entry Count",size:10],
                [name:"childCount",displayName:"Child Count",size:10],
                [name:"name",displayName:"Name",size:45],
        ]
        dumpStats(header, stats, columns)
    }

    //~---------------------------------------------< Traversor implementations >

    FluentIterable<Tree> getFilteringTraversor(Tree tree ){
        FluentIterable<Tree> itr = getTreeTraversor(tree)

        //Only include nodes upto /oak:index
        return getCountingTraversor(itr.filter(new Predicate<Tree>() {
            boolean apply(Tree t) {
                //Some property index seem to index oak:index as value
                int oakIndexCount = t.path.count('oak:index')
                if (oakIndexCount > 0){
                    if (t.path.endsWith('oak:index') && oakIndexCount == 1){
                        return true
                    } else {
                        return false
                    }
                } else {
                    return true
                }
            }
        }))
    }

    FluentIterable<Tree> getCountingTraversor(Tree tree) {
        return getTreeTraversor(tree).filter(countingPredicate)
    }

    FluentIterable<Tree> getCountingTraversor(FluentIterable<Tree> itr) {
        return itr.filter(countingPredicate)
    }

    static FluentIterable<Tree> getTreeTraversor(Tree t){
        def traversor = new TreeTraverser<Tree>(){
            Iterable<Tree> children(Tree root) {
                return root.children
            }
        }
       return traversor.preOrderTraversal(t)
    }

    //~--------------------------------------------< Text Table Template >

    def dumpStats(String header, def stats, def columns){
        StringWriter sw = new StringWriter()
        PrintWriter pw = new PrintWriter(sw)
        pw.println(header)
        pw.println()

        def ttf = new TemplateFactory()
        ttf.columns = columns

        pw.println(new SimpleTemplateEngine().createTemplate(ttf.template).make([rows:stats]).toString())
        return sw.toString()
    }

    class TemplateFactory {
        def columns = []

        @SuppressWarnings("GroovyAssignabilityCheck")
        @CompileStatic(TypeCheckingMode.SKIP)
        String getTemplate() { """
${columns.collect{ " <%print \"$it.displayName\".center($it.size)%> " }.join()}
${columns.collect{ " <%print \"_\"*$it.size %> " }.join()}
<% rows.each {%>${columns.collect{ " \${it.${it.name}.toString().padRight($it.size).substring(0,$it.size)} " }.join()}
<% } %>"""
        }
    }

    //~---------------------------------------< utils >

    static class DiffIterator extends AbstractIterator<String> {
        final PeekingIterator<String> peekB;
        final Iterator<String> b
        final Iterator<String> a

        DiffIterator(Iterable<String> a, Iterable<String> b){
            this(a.iterator(), b.iterator())
        }

        DiffIterator(Iterator<String> a, Iterator<String> b){
            this.b = b
            this.peekB = Iterators.peekingIterator(b)
            this.a = a
        }

        @Override
        protected String computeNext() {
            String diff = computeNextDiff();
            if (diff == null) {
                return endOfData();
            }
            return diff;
        }

        private String computeNextDiff() {
            if (!a.hasNext()) {
                return null;
            }

            //Marked finish the rest of all are part of diff
            if (!peekB.hasNext()) {
                return a.next();
            }

            String diff = null;
            while (a.hasNext() && diff == null) {
                diff = a.next();
                while (peekB.hasNext()) {
                    String marked = peekB.peek();
                    int comparisonResult = diff.compareTo(marked);
                    if (comparisonResult > 0) {
                        //Extra entries in marked. Ignore them and move on
                        peekB.next();
                    } else if (comparisonResult == 0) {
                        //Matching entry found in marked move past it. Not a
                        //dif candidate
                        peekB.next();
                        diff = null;
                        break;
                    } else {
                        //This entry is not found in marked entries
                        //hence part of diff
                        return diff;
                    }
                }
            }
            return diff;
        }
    }

    static String withSuffix(long count) {
        if (count < 1000) return "" + count;
        int exp = (int) (Math.log(count) / Math.log(1000));
        return String.format("%.1f %c",
                count / Math.pow(1000, exp),
                "kMGTPE".charAt(exp-1));
    }

    static String withSizeSuffix(long size){
        humanReadableByteCount(size)
    }
}

new RepoStats(nodeStore: session.store).dumpStats()

