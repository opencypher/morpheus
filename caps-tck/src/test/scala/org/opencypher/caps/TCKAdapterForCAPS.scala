package org.opencypher.caps

import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.caps.api.value.{CypherValue => CAPSValue}
import org.opencypher.caps.impl.exception.Raise
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.CypherValue

object TCKAdapterForCAPS {

  implicit class AsTckGraph(graph: CAPSGraph) extends Graph {
    override def execute(query: String, params: Map[String, CypherValue], queryType: QueryType): (Graph, Result) = {
      queryType match {
        case InitQuery =>
          // we don't support updates on this adapter
          Raise.notYetImplemented("update queries for CAPS graphs")
        case SideEffectQuery =>
          // this one is tricky, not sure how can do it without Cypher
          this -> CypherValueRecords.empty
        case ExecQuery =>
          // mapValues is lazy, so we force it for debug purposes
          val capsResult = graph.cypher(query, params.mapValues(CAPSValue(_)).view.force)
          val tckRecords = convertToTckStrings(capsResult.records)

          this -> tckRecords
      }
    }

    private def convertToTckStrings(records: CAPSRecords): StringRecords = {
      val header = records.header.fieldsInOrder.map(_.name).toList
      val rows = records.toLocalScalaIterator.map { cypherMap =>
        cypherMap.keys.map(k => k -> java.util.Objects.toString(cypherMap.get(k).get)).toMap
      }.toList

      StringRecords(header, rows)
    }
  }

}
