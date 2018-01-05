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
import com.google.common.collect.BiMap
import com.google.common.collect.HashBiMap
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames
import org.apache.lucene.document.Document
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.DocsEnum
import org.apache.lucene.index.Fields
import org.apache.lucene.index.IndexableField
import org.apache.lucene.index.MultiFields
import org.apache.lucene.index.Terms
import org.apache.lucene.index.TermsEnum
import org.apache.lucene.search.DocIdSetIterator
import org.apache.lucene.store.Directory
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.util.Bits

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
class LuceneIndexContentDumper{
    final String INDEX_PATH = "indexPath"
    private ArrayList<DocInfo> infos
    private static final BiMap<String, Integer> fieldIdMapping = HashBiMap.create()

    def dump(){
        File indexDir = getIndexDir()
        Directory dir = FSDirectory.open(indexDir)
        DirectoryReader reader = DirectoryReader.open(dir)

        println "Proceeding to dump index contents from ${indexDir.absolutePath}"

        Stopwatch w = Stopwatch.createStarted()
        File dumpFile = new File("index-contents.txt")

        reader.withCloseable {
            def numDocs = reader.numDocs()
            println "Collecting data for ${numDocs} docs"
            infos = new ArrayList<>(numDocs)

            collectPaths(reader)
            collectFieldNames(reader)
            dumpDocInfo(dumpFile)

            println "Dumped $numDocs documents contents in json format to ${dumpFile.absolutePath} in $w"
        }
    }

    def dumpDocInfo(File file) {
        file.withPrintWriter { pw ->
            infos.sort {it.path}
            infos.each {pw.println(it)}
        }
    }

    def collectFieldNames(DirectoryReader reader) {
        println "Proceeding to collect the field names per document"

        Bits liveDocs = MultiFields.getLiveDocs(reader)
        Fields fields = MultiFields.getFields(reader)
        fields.each {String fieldName ->
            Terms terms = fields.terms(fieldName)
            TermsEnum termsEnum = terms.iterator(null)

            while (termsEnum.next() != null) {
                DocsEnum docsEnum = termsEnum.docs(liveDocs, null, DocsEnum.FLAG_NONE)
                while(docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    int docId = docsEnum.docID()
                    DocInfo di = infos.get(docId)
                    assert di : "No DocInfo for docId : $docId"
                    di.fieldIds << getFieldId(fieldName)
                }
            }
        }
    }

    def collectPaths(DirectoryReader reader) {
        Stopwatch w = Stopwatch.createStarted()
        for (int i = 0; i < reader.numDocs(); i++) {
            Document d = reader.document(i)
            IndexableField path = d.getField(FieldNames.PATH)
            assert path : "No path field found for document $i"
            infos.add(i, new DocInfo(path.stringValue()))
        }
        println "Collected path details for ${reader.numDocs()} docs in $w"
    }

    private File getIndexDir() {
        String indexPath = System.getProperty(INDEX_PATH)
        assert indexPath: "No indexPath provided via system property '$INDEX_PATH'"

        File indexDir = new File(indexPath)
        assert indexDir.exists() && indexDir.isDirectory(): "Index path [${indexDir.absolutePath}] does not exist"
        return indexDir
    }

    static String getField(Integer fieldId) {
        return fieldIdMapping.inverse().get(fieldId)
    }

    static Integer getFieldId(String fieldName) {
        Integer id = fieldIdMapping.get(fieldName)
        if (!id) {
            id = fieldIdMapping.size() + 1
            fieldIdMapping.put(fieldName, id)
        }
        return id
    }

    @TupleConstructor
    static class DocInfo {
        final String path
        Set<Integer> fieldIds = new HashSet<>()

        String toString(){
            List<String> names = fieldIds.collect {LuceneIndexContentDumper.getField(it)}
            Collections.sort(names)
            return "$path|${names.join(',')}"
        }
    }
}

new LuceneIndexContentDumper().dump()


