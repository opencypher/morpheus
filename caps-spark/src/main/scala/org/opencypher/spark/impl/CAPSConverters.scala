package org.opencypher.spark.impl

import org.opencypher.okapi.api.graph.{CypherQueryPlans, CypherResult, CypherSession, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.physical.CAPSQueryPlans

object CAPSConverters {

  implicit class RichSession(val session: CypherSession) extends AnyVal {
    def asCaps: CAPSSession = session match {
      case caps: CAPSSession => caps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS sessions, got $session")
    }
  }

  implicit class RichPropertyGraph(val graph: PropertyGraph) extends AnyVal {
    def asCaps: CAPSGraph = graph match {
      case caps: CAPSGraph => caps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS graphs, got $graph")
    }
  }

  implicit class RichCypherResult(val result: CypherResult) extends AnyVal {
    def asCaps: CAPSResult = result match {
      case caps: CAPSResult => caps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS result, got $result")
    }
  }

  implicit class RichCypherRecords(val records: CypherRecords) extends AnyVal {
    def asCaps: CAPSRecords = records match {
      case caps: CAPSRecords => caps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS records, got $records")
    }
  }

  implicit class RichCypherPlans(val plans: CypherQueryPlans) extends AnyVal {
    def asCaps: CAPSQueryPlans = plans match {
      case caps: CAPSQueryPlans => caps
      case _ => throw UnsupportedOperationException(s"can only handle CAPS plans, got $plans")
    }
  }

}
