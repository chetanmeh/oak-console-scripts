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
import com.google.common.base.Stopwatch
import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.mongodb.DBObject
import groovy.transform.CompileStatic
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils

@CompileStatic
class TraversalRateEstimator {
    final MongoConnection conn
    final Meter meter
    final Stopwatch watch = Stopwatch.createStarted()
    long traversalCount = 0

    TraversalRateEstimator(Whiteboard wb){
        conn = WhiteboardUtils.getService(wb, MongoConnection.class)
        MetricRegistry registry = WhiteboardUtils.getService(wb, MetricRegistry.class)
        assert registry : "Use --metrics option to enable metrics for this script"
        meter = registry.meter("traversal-meter")
    }

    def readAll(){
        DBCollection col = conn.DB.getCollection("nodes")

        DBCursor cursor = col.find()
        while (cursor.hasNext()) {
            DBObject obj = cursor.next()
            tick(obj)
        }
        println "Done traversal of $traversalCount"
    }

    def tick(DBObject obj){
        meter.mark()
        if (++traversalCount % 10000 == 0) {
            double rate = meter.getFiveMinuteRate()
            String id = obj.get("_id")
            String formattedRate = String.format("%1.2f nodes/s, %1.2f nodes/hr", rate, rate * 3600)
            println "[$watch] Traversed #$traversalCount $id [$formattedRate]"
        }
    }
}

new TraversalRateEstimator(session.whiteboard).readAll()

