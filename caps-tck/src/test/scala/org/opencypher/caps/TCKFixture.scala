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
package org.opencypher.caps

import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.exception.NotImplementedException
import org.opencypher.caps.api.value.CAPSValue
import org.opencypher.caps.impl.record.CypherRecords
import org.opencypher.caps.impl.spark.CAPSGraph
import org.opencypher.caps.test.support.creation.caps.CAPSGraphFactory
import org.opencypher.caps.test.support.creation.propertygraph.Neo4jPropertyGraphFactory
import org.opencypher.tools.tck.api._
import org.opencypher.tools.tck.values._

import scala.io.Source

case class TCKGraph(capsGraphFactory: CAPSGraphFactory, graph: CAPSGraph)(implicit caps: CAPSSession) extends Graph {

  override def execute(query: String, params: Map[String, CypherValue], queryType: QueryType): (Graph, Result) = {
    queryType match {
      case InitQuery =>
        val capsGraph = capsGraphFactory(Neo4jPropertyGraphFactory(query, params.mapValues(tckCypherValueToScala)))
        copy(graph = capsGraph) -> CypherValueRecords.empty
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

  private def convertToTckStrings(records: CypherRecords): StringRecords = {
    val header = records.header.fieldsInOrder.toList
    val rows = records.iterator.map { cypherMap =>
      cypherMap.keys.map(k => k -> java.util.Objects.toString(cypherMap.get(k).get)).toMap
    }.toList

    StringRecords(header, rows)
  }

  private def tckCypherValueToScala(cypherValue: CypherValue): Any = cypherValue match {
    case CypherString(v)               => v
    case CypherInteger(v)              => v
    case CypherFloat(v)                => v
    case CypherBoolean(v)              => v
    case CypherProperty(key, value)    => key -> tckCypherValueToScala(value)
    case CypherPropertyMap(properties) => properties.mapValues(tckCypherValueToScala)
    case l: CypherList                 => l.elements.map(tckCypherValueToScala)
    case CypherNull                    => null
    case other =>
      throw NotImplementedException(s"Converting Cypher value $cypherValue of type `${other.getClass.getSimpleName}`")
  }
}

object ScenarioBlacklist {
  private lazy val blacklist: Set[String] = {
    val blacklistIter = Source.fromFile(getClass.getResource("scenario_blacklist").toURI).getLines().toSeq
    val blacklistSet = blacklistIter.toSet

    lazy val errorMessage =
      s"Blacklist contains duplicate scenarios ${blacklistIter.groupBy(identity).filter(_._2.lengthCompare(1) > 0).keys.mkString("\n")}"
    assert(blacklistIter.lengthCompare(blacklistSet.size) == 0, errorMessage)
    blacklistSet
  }

  def contains(name: String): Boolean = blacklist.contains(name)
}
