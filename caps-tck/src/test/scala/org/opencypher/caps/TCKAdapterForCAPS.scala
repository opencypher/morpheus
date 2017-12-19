/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
