package org.opencypher.caps

import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords}
import org.opencypher.caps.api.value.{CypherValue => CAPSValue}
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values.CypherValue

object TCKAdapterForCAPS {

  implicit class AsTckGraph(graph: CAPSGraph) extends Graph {

    override def execute(query: String, params: Map[String, CypherValue], queryType: QueryType): (Graph, Result) = {

      queryType match {
        case InitQuery =>
          // we don't support any updates at all
          // redirect to GDL or neo4j-harness or w/e
          this -> CypherValueRecords.empty
        case SideEffectQuery =>
          // this one is tricky, not sure how can do it without Cypher
          this -> CypherValueRecords.empty
        case ExecQuery =>
          // mapValues is lazy, so we force it for debug purposes
          val capsResult = graph.cypher(query, params.mapValues(CAPSValue(_)).view.force)
          val tckRecords = convertToTckStrings(capsResult.records)
          val tckGraph: Graph = capsResult.graphs.values.headOption.map(AsTckGraph).getOrElse(this)
          (tckGraph, tckRecords)
      }
    }

    private def convertToTckStrings(records: CAPSRecords): StringRecords = {
      val header = records.header.fieldsInOrder.map(_.name).toList
      val rows = records.toLocalScalaIterator.map { cypherMap =>
        cypherMap.keys.map(k => k -> cypherMap.get(k).orNull.toString).toMap
      }.toList

      StringRecords(header, rows)
    }
  }

}
