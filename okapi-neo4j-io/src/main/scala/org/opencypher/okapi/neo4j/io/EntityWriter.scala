package org.opencypher.okapi.neo4j.io

import java.util

import org.apache.logging.log4j.scala.Logging
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.CypherRelationship
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._

object EntityWriter extends Logging {

  def writeNodes(
    nodes: Iterator[Properties],
    mapping: Array[String],
    config: Neo4jConfig,
    labels: Set[String],
    batchSize: Int = 1000
  ): Unit = {
    val labelString = labels.mkString(":")

    config.withSession { session =>
      nodes.grouped(batchSize).foreach { batch =>

        val list = new Array[PropertyMap](batch.size)
        batch.indices.foreach(i => list(i) = new PropertyMap(batch(i), mapping))

        val params = new java.util.HashMap[String, AnyRef]()
        params.put("batch", list)

        val createQ =
          s"""
             |UNWIND {batch} as row
             |CREATE (n:$labelString)
             |SET n += row
         """.stripMargin

        session.run(createQ, params).consume()
      }
    }
  }

//  def writeRelationships(
//    rels: Iterator[Properties],
//    mapping: Array[String],
//    config: Neo4jConfig,
//    labels: Set[String],
//    batchSize: Int = 1000
//  ): Unit = {
//
//    config.withSession { session =>
//
//      val values = rels.map(rel => Map(
//        "from" -> rel.startId,
//        "to" -> rel.endId,
//        "properties" -> rel.properties.unwrap.updated(metaPropertyKey, rel.id))).toList.asJava
//
//      val params = Map("batch" -> values).asJava
//
//      val createQ =
//        s"""
//           |UNWIND {batch} as row
//           |MATCH (from {$metaPropertyKey : row.from})
//           |MATCH (to {$metaPropertyKey : row.to})
//           |CREATE (from)-[rel:${relType.get}]->(to)
//           |SET rel += row.properties
//         """.stripMargin
//
//      session.run(createQ, params).consume()
//    }
//  }

  def writeRelationships[Rel <: CypherRelationship[_]](
    relIterator: Iterator[Rel],
    config: Neo4jConfig,
    metaLabel: Option[String] = None
  ): Unit = {

    val queryStringBuilder = new StringBuilder()

    val graphLabelString = metaLabel.mkString(":", "", "")
    val matchString = s"MATCH (a$graphLabelString), (b$graphLabelString)"
    val seperatorString = "\nWITH 1 as __sep__\n"

    config.withSession { session =>

      relIterator.grouped(1000).foreach { rels =>
        queryStringBuilder.clear()

        rels.foreach { rel =>
          val properties = rel.properties.updated(metaPropertyKey, CypherValue(rel.id))
          val propertyString = properties.toCypherString

          queryStringBuilder ++=
            matchString ++=
            "\nWHERE a." ++= metaPropertyKey ++= "=" ++= rel.startId.toString ++= " AND b." ++= metaPropertyKey ++= "=" ++= rel.endId.toString ++=
            "\nCREATE (a)-[r:" ++= rel.relType ++= " " ++= propertyString ++= "]->(b)" ++=
            seperatorString
        }

        // remove the last separator
        val stringLength = queryStringBuilder.size
        val queryString = queryStringBuilder.substring(0, stringLength - seperatorString.length)

        logger.debug(s"Executing query on Neo4j: ${queryString.replaceAll("\n", " ")}")
        session.run(queryString).consume()
      }
    }
  }
}


trait Properties {
  def getProperty(i: Int): AnyRef
  def numberOfProperties: Int
}

class PropertyMap(properties: Properties, mapping: Array[String]) extends java.util.Map[String, AnyRef] {
  assert(properties.numberOfProperties == mapping.length)

  override def size(): Int = mapping.length
  override def isEmpty: Boolean = mapping.isEmpty
  override def containsKey(o: scala.Any): Boolean = mapping.contains(o)

  override def containsValue(o: scala.Any): Boolean = {
    var i = 0
    while(i < properties.numberOfProperties) {
      if(properties.getProperty(i) == o) return true
      i += 1
    }
    false
  }

  override def get(o: scala.Any): AnyRef = o match {
    case s: String => properties.getProperty(mapping.indexOf(s))
    case _ => null
  }

  override def keySet(): util.Set[String] = {
    val list = new java.util.HashSet[String](mapping.length)
    mapping.foreach(list.add)
    list
  }

  override def values(): util.Collection[AnyRef] = {
    val list = new java.util.ArrayList[AnyRef](mapping.length)
    mapping.indices.foreach(x => list.add(properties.getProperty(x)))
    list
  }

  override def entrySet(): util.Set[java.util.Map.Entry[String, AnyRef]] = {
    val set = new java.util.HashSet[java.util.Map.Entry[String, AnyRef]](mapping.length)
    mapping.indices.foreach {i =>
      set.add(new util.AbstractMap.SimpleEntry[String, AnyRef](mapping(i), properties.getProperty(i)))
    }
    set
  }

  override def put(k: String, v: AnyRef): AnyRef = throw new UnsupportedOperationException("Immutable")
  override def remove(o: scala.Any): AnyRef = throw new UnsupportedOperationException("Immutable")
  override def putAll(map: util.Map[_ <: String, _ <: AnyRef]): Unit = throw new UnsupportedOperationException("Immutable")
  override def clear(): Unit = throw new UnsupportedOperationException("Immutable")

  override def toString: String = {
    mapping.indices.map { i =>
      s"${mapping(i)}: ${properties.getProperty(i)}"
    }.mkString("{",", ","}")
  }
}