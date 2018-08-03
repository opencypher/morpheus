package org.opencypher.okapi.neo4j.io

import org.apache.logging.log4j.scala.Logging
import org.neo4j.driver.internal.value.{ListValue, MapValue}
import org.neo4j.driver.v1.{Statement, Value}
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

object EntityWriter extends Logging {

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
      .map{ case (key, i) => s"SET n.$key = row[$i]" }
      .mkString("\n")

    val createQ =
      s"""
         |UNWIND {batch} as row
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
      .map{ case (key, i) => s"SET rel.$key = row[$i]" }
      .mkString("\n")

    val nodeLabelString = nodeLabel.map(l => s":$l").getOrElse("")

    val createQ =
      s"""
         |UNWIND $$batch as row
         |MATCH (from$nodeLabelString {$metaPropertyKey : row[$startNodeIndex]})
         |MATCH (to$nodeLabelString {$metaPropertyKey : row[$endNodeIndex]})
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
