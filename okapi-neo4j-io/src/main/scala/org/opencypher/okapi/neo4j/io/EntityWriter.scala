package org.opencypher.okapi.neo4j.io

import org.apache.logging.log4j.scala.Logging
import org.neo4j.driver.internal.value.{ListValue, MapValue}
import org.neo4j.driver.v1.{Statement, Value, Values}
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

object EntityWriter extends Logging {

  def writeNodes(
    nodes: Iterator[EntityRow],
    propertyMapping: Array[String],
    config: Neo4jConfig,
    labels: Set[String],
    batchSize: Int = 1000
  ): Unit = {
    val labelString = labels.mkString(":")

    val reuseMap = new java.util.HashMap[String, Value]
    val reuseParameters = new MapValue(reuseMap)

    val createQ =
      s"""
         |UNWIND {batch} as row
         |CREATE (n:$labelString)
         |SET n += row
         """.stripMargin

    val reuseStatement = new Statement(createQ, reuseParameters)

    config.withSession { session =>
      val batches = nodes.grouped(batchSize)
      while (batches.hasNext) {
        val batch = batches.next()
        val list = new Array[MapValue](batch.size)

        var i = 0
        batch.foreach { row =>
          val rowParameters = new java.util.HashMap[String, Value]()
          var j = 0
          while (j < propertyMapping.length) {
            val key = propertyMapping(j)
            if (key != null) {
              rowParameters.put(key, Values.value(row.getValue(j)))
            }
            j += 1
          }
          list(i) = new MapValue(rowParameters)
          i += 1
        }

        reuseMap.put("batch", new ListValue(list: _*))

        reuseStatement.withUpdatedParameters(reuseParameters)

        session.run(reuseStatement).consume()
      }
    }
  }

  def writeNodesList(
    nodes: Iterator[EntityRow],
    propertyMapping: Array[String],
    config: Neo4jConfig,
    labels: Set[String],
    batchSize: Int = 1000
  ): Unit = {
    val labelString = labels.mkString(":")

    val reuseMap = new java.util.HashMap[String, Value]
    val reuseParameters = new MapValue(reuseMap)

    val setStatements = propertyMapping
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

    val reuseStatement = new Statement(createQ, reuseParameters)

    config.withSession { session =>
      val batches = nodes.grouped(batchSize)
      while (batches.hasNext) {
        val batch = batches.next()
        val list = new Array[ListValue](batch.size)

        var i = 0
        batch.foreach { row =>
          list(i) = row.asListValue
          i += 1
        }

        reuseMap.put("batch", new ListValue(list: _*))

        reuseStatement.withUpdatedParameters(reuseParameters)

        session.run(reuseStatement).consume()
      }
    }
  }

  def writeRelationships(
    relationships: Iterator[EntityRow],
    startNodeIndex: Int,
    endNodeIndex: Int,
    propertyMapping: Map[String, Int],
    config: Neo4jConfig,
    relType: String,
    batchSize: Int = 1000
  ): Unit = {

    //    config.withSession { session =>
    //      relationships.grouped(batchSize).foreach { batch =>
    //
    //        val list = new Array[RelationshipMap](batch.size)
    //        batch.indices.foreach { i =>
    //          list(i) = new RelationshipMap(startNodeIndex, endNodeIndex, new PropertyMap(batch(i), propertyMapping))
    //        }
    //
    //        val params = new java.util.HashMap[String, AnyRef]()
    //        params.put("batch", list)
    //
    //        val createQ =
    //          s"""
    //             |UNWIND {batch} as row
    //             |MATCH (from {$metaPropertyKey : row.$START_NODE_KEY})
    //             |MATCH (to {$metaPropertyKey : row.$END_NODE_KEY})
    //             |CREATE (from)-[rel:$relType]->(to)
    //             |SET rel += row.$PROPERTIES_KEY
    //         """.stripMargin
    //
    //        session.run(createQ, params).consume()
    //      }
    //    }
  }
}

trait EntityRow {
  def getValue(i: Int): AnyRef
  def size: Int
  def asListValue: ListValue
}