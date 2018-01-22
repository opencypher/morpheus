/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.api.spark

import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.exception.UnsupportedOperationException
import org.opencypher.caps.api.graph.{CypherGraph, CypherResult, CypherSession}
import org.opencypher.caps.api.record.{CypherRecordHeader, CypherRecords}
import org.opencypher.caps.impl.record.RecordHeader

object CAPSConverters {

  implicit class RichSession(session: CypherSession) {
    def asCaps: CAPSSession = session match {
      case caps: CAPSSession => caps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS sessions, got $session")
    }
  }

  implicit class RichPropertyGraph(graph: CypherGraph) {
    def asCaps: CAPSGraph = graph match {
      case caps: CAPSGraph => caps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS graphs, got $graph")
    }
  }

  implicit class RichCypherResult(result: CypherResult) {
    def asCaps: CAPSResult = result match {
      case caps: CAPSResult => caps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS result, got $result")
    }
  }

  implicit class RichCypherRecords(records: CypherRecords) {
    def asCaps: CAPSRecords = records match {
      case caps: CAPSRecords => caps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS records, got $records")
    }
  }

  implicit class RichCypherHeader(header: CypherRecordHeader) {
    def asCaps: RecordHeader = header match {
      case caps: RecordHeader => caps
      case _ => throw UnsupportedOperationException(s"can only handle CORA record header, got $header")
    }
  }

}
