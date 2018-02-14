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
package org.opencypher.caps.cosc.impl

import org.opencypher.caps.api.graph.{CypherResult, CypherSession, PropertyGraph}
import org.opencypher.caps.api.table.CypherRecords
import org.opencypher.caps.impl.exception.UnsupportedOperationException
import org.opencypher.caps.impl.table.RecordHeader

object COSCConverters {

  implicit class RichSession(session: CypherSession) {
    def asCosc: COSCSession = session match {
      case cosc: COSCSession => cosc
      case _                 => throw UnsupportedOperationException(s"can only handle COSC sessions, got $session")
    }
  }

  implicit class RichPropertyGraph(graph: PropertyGraph) {
    def asCosc: COSCGraph = graph match {
      case cosc: COSCGraph => cosc
      case _               => throw UnsupportedOperationException(s"can only handle COSC graphs, got $graph")
    }
  }

  implicit class RichCypherResult(result: CypherResult) {
    def asCosc: COSCResult = result match {
      case cosc: COSCResult => cosc
      case _                => throw UnsupportedOperationException(s"can only handle COSC result, got $result")
    }
  }

  implicit class RichCypherRecords(records: CypherRecords) {
    def asCosc: COSCRecords = records match {
      case cosc: COSCRecords => cosc
      case _                 => throw UnsupportedOperationException(s"can only handle COSC records, got $records")
    }
  }

}
