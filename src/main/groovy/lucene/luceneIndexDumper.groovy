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

import com.google.common.base.Joiner
import com.google.common.base.Stopwatch
import com.google.common.collect.AbstractIterator
import com.google.common.collect.Iterators
import com.google.common.collect.Lists
import com.google.common.collect.PeekingIterator
import com.google.common.collect.Sets
import groovy.transform.CompileStatic
import org.apache.commons.io.FileUtils
import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.DocsEnum
import org.apache.lucene.index.Fields
import org.apache.lucene.index.MultiFields
import org.apache.lucene.index.Terms
import org.apache.lucene.index.TermsEnum
import org.apache.lucene.search.DocIdSetIterator
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.Bits
import org.apache.lucene.util.FixedBitSet

import static com.google.common.base.Charsets.UTF_8
import static org.apache.jackrabbit.oak.commons.sort.ExternalSort.mergeSortedFiles
import static org.apache.jackrabbit.oak.commons.sort.ExternalSort.sortInBatch

@CompileStatic
class LuceneIndexContentDumper{
    final String INDEX_PATH = "indexPath"
    final int MAX_TMP_FILES = 1024
    final long MAX_MEMOERY = 1*1024*1024*1024

    static final Set<String> ignoredFields = Sets.newHashSet(":ancestors", ":depth", ":path")

    def dump() {
        File indexDir = getIndexDir()
        Directory dir = FSDirectory.open(indexDir)
        DirectoryReader reader = DirectoryReader.open(dir)


        Stopwatch w = Stopwatch.createStarted()
        File dumpFile = new File("index-contents.tmp.txt")
        File sortedDumpFile = new File("index-contents.txt")

        println "Iterating fields"
        reader.withCloseable {
            int numDocs = collectFields(dumpFile, reader)

            println "Dumped $numDocs documents contents to ${dumpFile.absolutePath} in $w"
        }

        Comparator<String> fileComparator = new Comparator<String>() {
            @Override
            int compare(String s1, String s2) {
                return s1.substring(0, s1.indexOf('|')).compareTo(s2.substring(0, s2.indexOf('|')))
            }
        }

        w.reset()
        w.start()
        println "Going to sort"
        mergeSortedFiles(
                sortInBatch(dumpFile, fileComparator, MAX_TMP_FILES, MAX_MEMOERY, UTF_8, null, false),
                sortedDumpFile, fileComparator)
        println "Sorted output put in ${sortedDumpFile.absolutePath} in $w"

        FileUtils.deleteQuietly(dumpFile)
    }

    int collectFields(File file, DirectoryReader reader) {
        Bits liveDocs = MultiFields.getLiveDocs(reader)
        Fields fields = MultiFields.getFields(reader)

        Stopwatch w = Stopwatch.createStarted()

        List<FieldsDocIdIterator> fldDocIdIters = Lists.newArrayList()
        fields.each {String fieldName ->
            if (!ignoredFields.contains(fieldName)) {
                FieldsDocIdIterator fldDocIter = new FieldsDocIdIterator(liveDocs, fields, fieldName, reader.maxDoc())
                fldDocIdIters.add(fldDocIter)
            }
        }
        println "numFields: ${fldDocIdIters.size()}"

        IndexDocInfoItearator idxDocInfos = new IndexDocInfoItearator(fldDocIdIters)
        println "Preparing iterators took $w"

        println "Proceeding to dump index contents from ${indexDir.absolutePath}"

        int numDocs = 0
        file.withPrintWriter { pw ->
            idxDocInfos.each { docInfo ->
                String path = reader.document(docInfo.docId).get(FieldNames.PATH)

                numDocs++
                pw.println "$path|${Joiner.on(',').join(docInfo.fieldNames)}"
            }
        }

        return numDocs
    }

    private File getIndexDir() {
        String indexPath = System.getProperty(INDEX_PATH)
        assert indexPath: "No indexPath provided via system property '$INDEX_PATH'"

        File indexDir = new File(indexPath)
        assert indexDir.exists() && indexDir.isDirectory(): "Index path [${indexDir.absolutePath}] does not exist"
        return indexDir
    }

    static class FieldsDocIdIterator extends AbstractIterator<FieldNameDocIdStruct> {
        private Bits liveDocs
        private FixedBitSet fbs
        private DocIdSetIterator docIdIter
        private String fieldName
        private Terms terms

        FieldsDocIdIterator(Bits liveDocs, Fields fields, String fieldName, int maxDoc) {
            this.liveDocs = liveDocs
            this.fbs = new FixedBitSet(maxDoc)
            this.fieldName = fieldName
            this.terms = fields.terms(fieldName)

            TermsEnum termsEnum = terms.iterator(null)

            while (termsEnum.next() != null) {
                new TermsDocIdIterator(termsEnum.docs(liveDocs, null, DocsEnum.FLAG_NONE)).each { docId ->
                    fbs.set(docId)
                }
            }

            println "Field $fieldName has ${fbs.cardinality()} documents"

            docIdIter = fbs.iterator()
        }

        @Override
        protected FieldNameDocIdStruct computeNext() {
            if(docIdIter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                return new FieldNameDocIdStruct(fieldName, docIdIter.docID())
            } else {
                return endOfData()
            }
        }
    }

    static class TermsDocIdIterator extends AbstractIterator<Integer> {
        private DocsEnum docsEnum

        TermsDocIdIterator(DocsEnum docsEnum) {
            this.docsEnum = docsEnum
        }

        @Override
        protected Integer computeNext() {
            if(docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                return docsEnum.docID()
            } else {
                return endOfData()
            }
        }
    }

    static class DocInfo {
        final int docId
        final List<String> fieldNames

        DocInfo(int docId, List<String> fieldNames) {
            this.docId = docId
            this.fieldNames = fieldNames
        }
    }

    static class IndexDocInfoItearator extends AbstractIterator<DocInfo> {
        private PeekingIterator<FieldNameDocIdStruct> mergedIter
        private Integer lastDocId = null

        IndexDocInfoItearator(Iterable<? extends Iterator<FieldNameDocIdStruct>> iterators) {
            mergedIter = Iterators.peekingIterator(Iterators.mergeSorted(iterators, new Comparator<FieldNameDocIdStruct>() {
                @Override
                int compare(FieldNameDocIdStruct i1, FieldNameDocIdStruct i2) {
                    return i1 <=> i2
                }
            }))
        }

        @Override
        protected DocInfo computeNext() {
            if (!mergedIter.hasNext()) {
                return endOfData()
            }
            List<String> fieldNames = Lists.newArrayList()

            FieldNameDocIdStruct next = mergedIter.peek()
            if (lastDocId == null) {
                lastDocId = next.docId
            }

            while (lastDocId.equals(next.docId) && mergedIter.hasNext()) {
                fieldNames.add(next.fieldName)
                mergedIter.next() // consume

                if (mergedIter.hasNext()) {
                    next = mergedIter.peek()
                }
            }

            DocInfo ret = new DocInfo(lastDocId, fieldNames)
            lastDocId = null
            return ret
        }
    }

    static class FieldNameDocIdStruct implements Comparable<FieldNameDocIdStruct> {
        final String fieldName
        final int docId

        FieldNameDocIdStruct(String fieldName, int docId) {
            this.fieldName = fieldName
            this.docId = docId
        }

        @Override
        int compareTo(FieldNameDocIdStruct that) {
            int docIdCmp = this.docId.compareTo(that.docId)
            return docIdCmp != 0 ? docIdCmp : this.fieldName.compareTo(that.fieldName)
        }
    }
}

new LuceneIndexContentDumper().dump()
