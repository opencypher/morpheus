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

import org.opencypher.caps.api.exception.NotImplementedException
import org.opencypher.caps.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.api.value.CypherValue.{CypherList => CAPSCypherList, CypherMap => CAPSCypherMap, CypherValue => CAPSCypherValue}
import org.opencypher.caps.impl.record.CypherRecords
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.ir.impl.typer.exception.TypingException
import org.opencypher.caps.test.support.creation.TestGraphFactory
import org.opencypher.caps.test.support.creation.propertygraph.Neo4jPropertyGraphFactory
import org.opencypher.tools.tck.api.{ExecutionFailed, _}
import org.opencypher.tools.tck.constants.{TCKErrorDetails, TCKErrorPhases, TCKErrorTypes}
import org.opencypher.tools.tck.values.{CypherValue => TCKCypherValue, _}

import scala.io.Source
import scala.util.{Failure, Success, Try}

case class TCKGraph[C <: CypherSession](capsGraphFactory: TestGraphFactory[C], graph: PropertyGraph)(implicit caps: C) extends Graph {

  override def execute(query: String, params: Map[String, TCKCypherValue], queryType: QueryType): (Graph, Result) = {
    queryType match {
      case InitQuery =>
        val capsGraph = capsGraphFactory(Neo4jPropertyGraphFactory(query, params.mapValues(tckValueToCAPSValue))).asCaps
        copy(graph = capsGraph) -> CypherValueRecords.empty
      case SideEffectQuery =>
        // this one is tricky, not sure how can do it without Cypher
        this -> CypherValueRecords.empty
      case ExecQuery =>
        // mapValues is lazy, so we force it for debug purposes
        val capsResult = Try(graph.cypher(query, params.mapValues(tckValueToCAPSValue).view.force))
        capsResult match {
          case Success(r) => this -> convertToTckStrings(r.records)
          case Failure(e) =>
            val phase = TCKErrorPhases.RUNTIME // We have no way to detect errors during compile time yet
            e match {
              case t: TypingException => this ->
                ExecutionFailed(TCKErrorTypes.TYPE_ERROR, phase, TCKErrorDetails.INVALID_ARGUMENT_VALUE)
            }
        }
    }
  }

  private def convertToTckStrings(records: CypherRecords): StringRecords = {
    val header = records.header.fieldsInOrder.toList
    val rows: List[Map[String, String]] = records.iterator.map { cypherMap: CAPSCypherMap =>
      cypherMap.keys.map(k => k -> cypherMap(k).toCypherString).toMap
    }.toList
    StringRecords(header, rows)
  }

  private def tckValueToCAPSValue(cypherValue: TCKCypherValue): CAPSCypherValue = cypherValue match {
    case CypherString(v) => CypherValue(v)
    case CypherInteger(v) => CypherValue(v)
    case CypherFloat(v) => CypherValue(v)
    case CypherBoolean(v) => CypherValue(v)
    case CypherProperty(key, value) => CAPSCypherMap(key -> tckValueToCAPSValue(value))
    case CypherPropertyMap(properties) => CAPSCypherMap(properties.mapValues(tckValueToCAPSValue))
    case l: CypherList => CAPSCypherList(l.elements.map(tckValueToCAPSValue))
    case CypherNull => CypherValue(null)
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
