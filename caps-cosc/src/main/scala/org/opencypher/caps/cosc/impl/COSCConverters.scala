package org.opencypher.caps.cosc.impl

import org.opencypher.caps.api.exception.UnsupportedOperationException
import org.opencypher.caps.api.graph.{CypherResult, CypherSession, PropertyGraph}
import org.opencypher.caps.impl.record.{CypherRecordHeader, CypherRecords, RecordHeader}

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

  implicit class RichCypherHeader(header: CypherRecordHeader) {
    def asCosc: RecordHeader = header match {
      case cosc: RecordHeader => cosc
      case _                  => throw UnsupportedOperationException(s"can only handle CORA record header, got $header")
    }
  }

}
