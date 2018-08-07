/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.neo4j.io

import org.apache.logging.log4j.scala.Logging
import org.neo4j.driver.internal.value.{ListValue, MapValue}
import org.neo4j.driver.v1.{Statement, Value}
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

object EntityWriter extends Logging {

  private val ROW_IDENTIFIER = "row"

  def writeNodes[T](
    nodes: Iterator[T],
    rowMapping: Array[String],
    config: Neo4jConfig,
    labels: Set[String],
    batchSize: Int = 1000
  )(rowToListValue: T => ListValue): Unit = {
    val labelString = labels.mkString(":")

    val setStatements = rowMapping
      .zipWithIndex
      .filterNot(_._1 == null)
      .map{ case (key, i) => s"SET n.$key = $ROW_IDENTIFIER[$i]" }
      .mkString("\n")

    val createQ =
      s"""
         |UNWIND $$batch AS $ROW_IDENTIFIER
         |CREATE (n:$labelString)
         |$setStatements
         """.stripMargin

    writeEntities(nodes, rowMapping, createQ, config, batchSize)(rowToListValue)
  }

  def writeRelationships[T](
    relationships: Iterator[T],
    startNodeIndex: Int,
    endNodeIndex: Int,
    rowMapping: Array[String],
    config: Neo4jConfig,
    relType: String,
    nodeLabel: Option[String],
    batchSize: Int = 1000
  )(rowToListValue: T => ListValue): Unit = {
    val setStatements = rowMapping
      .zipWithIndex
      .filterNot(_._1 == null)
      .map{ case (key, i) => s"SET rel.$key = $ROW_IDENTIFIER[$i]" }
      .mkString("\n")

    val nodeLabelString = nodeLabel.map(l => s":$l").getOrElse("")

    val createQ =
      s"""
         |UNWIND $$batch AS $ROW_IDENTIFIER
         |MATCH (from$nodeLabelString {$metaPropertyKey : $ROW_IDENTIFIER[$startNodeIndex]})
         |MATCH (to$nodeLabelString {$metaPropertyKey : $ROW_IDENTIFIER[$endNodeIndex]})
         |CREATE (from)-[rel:$relType]->(to)
         |$setStatements
         """.stripMargin

    writeEntities(relationships, rowMapping, createQ, config, batchSize)(rowToListValue)
  }

  private def writeEntities[T](
    entities: Iterator[T],
    rowMapping: Array[String],
    query: String,
    config: Neo4jConfig,
    batchSize: Int = 1000
  )(rowToListValue: T => ListValue): Unit = {
    val reuseMap = new java.util.HashMap[String, Value]
    val reuseParameters = new MapValue(reuseMap)
    val reuseStatement = new Statement(query, reuseParameters)

    config.withSession { session =>
      val batches = entities.grouped(batchSize)
      while (batches.hasNext) {
        val batch = batches.next()
        val rowParameters = new Array[ListValue](batch.size)

        batch.zipWithIndex.foreach { case (row, i) => rowParameters(i) = rowToListValue(row) }

        reuseMap.put("batch", new ListValue(rowParameters: _*))

        reuseStatement.withUpdatedParameters(reuseParameters)

        session.run(reuseStatement).consume()
      }
    }
  }
}
