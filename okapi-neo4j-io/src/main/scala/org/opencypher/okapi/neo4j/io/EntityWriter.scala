package org.opencypher.okapi.neo4j.io

import java.util

import org.apache.logging.log4j.scala.Logging
import org.opencypher.okapi.neo4j.io.Neo4jHelpers.Neo4jDefaults._
import org.opencypher.okapi.neo4j.io.Neo4jHelpers._
import org.opencypher.okapi.neo4j.io.RelationshipMap._

object EntityWriter extends Logging {

  def writeNodes(
    nodes: Iterator[EntityRow],
    propertyMapping: Map[String, Int],
    config: Neo4jConfig,
    labels: Set[String],
    batchSize: Int = 1000
  ): Unit = {
    val labelString = labels.mkString(":")

    config.withSession { session =>
      nodes.grouped(batchSize).foreach { batch =>

        val list = new Array[PropertyMap](batch.size)
        batch.indices.foreach(i => list(i) = new PropertyMap(batch(i), propertyMapping))

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

  def writeRelationships(
    relationships: Iterator[EntityRow],
    startNodeIndex: Int,
    endNodeIndex: Int,
    propertyMapping: Map[String, Int],
    config: Neo4jConfig,
    relType: String,
    batchSize: Int = 1000
  ): Unit = {

    config.withSession { session =>
      relationships.grouped(batchSize).foreach { batch =>

        val list = new Array[RelationshipMap](batch.size)
        batch.indices.foreach { i =>
          list(i) = new RelationshipMap(startNodeIndex, endNodeIndex, new PropertyMap(batch(i), propertyMapping))
        }

        val params = new java.util.HashMap[String, AnyRef]()
        params.put("batch", list)

        val createQ =
          s"""
             |UNWIND {batch} as row
             |MATCH (from {$metaPropertyKey : row.$START_NODE_KEY})
             |MATCH (to {$metaPropertyKey : row.$END_NODE_KEY})
             |CREATE (from)-[rel:$relType]->(to)
             |SET rel += row.$PROPERTIES_KEY
         """.stripMargin

        session.run(createQ, params).consume()
      }
    }
  }
}

trait EntityRow {
  def getValue(i: Int): AnyRef
  def size: Int
}

class PropertyMap(val properties: EntityRow, val mapping: Map[String, Int]) extends java.util.Map[String, AnyRef] {

  override def size(): Int = mapping.size
  override def isEmpty: Boolean = mapping.isEmpty
  override def entrySet(): util.Set[java.util.Map.Entry[String, AnyRef]] = {
    val set = new java.util.HashSet[java.util.Map.Entry[String, AnyRef]](mapping.size)
    mapping.foreach {
      case (key, i) => set.add(new util.AbstractMap.SimpleEntry[String, AnyRef](key, properties.getValue(i)))
    }
    set
  }

  override def containsKey(o: scala.Any): Boolean = throw new UnsupportedOperationException("Not Implemented")
  override def containsValue(o: scala.Any): Boolean = throw new UnsupportedOperationException("Not Implemented")
  override def get(o: scala.Any): AnyRef = throw new UnsupportedOperationException("Not Implemented")
  override def keySet(): util.Set[String] = throw new UnsupportedOperationException("Not Implemented")
  override def values(): util.Collection[AnyRef] = throw new UnsupportedOperationException("Not Implemented")

  override def put(k: String, v: AnyRef): AnyRef = throw new UnsupportedOperationException("Immutable")
  override def remove(o: scala.Any): AnyRef = throw new UnsupportedOperationException("Immutable")
  override def putAll(map: util.Map[_ <: String, _ <: AnyRef]): Unit = throw new UnsupportedOperationException("Immutable")
  override def clear(): Unit = throw new UnsupportedOperationException("Immutable")
}

object RelationshipMap {
  val START_NODE_KEY = "from"
  val END_NODE_KEY = "to"
  val PROPERTIES_KEY = "properties"
}

class RelationshipMap(startNodeIndex: Int, endNodeIndex: Int, propertyMap: PropertyMap) extends java.util.Map[String, AnyRef] {
  override def size(): Int = 3
  override def isEmpty: Boolean = false
  override def entrySet(): util.Set[java.util.Map.Entry[String, AnyRef]] = {
    val properties = propertyMap.properties
    val set = new java.util.HashSet[java.util.Map.Entry[String, AnyRef]](3)
    set.add(new util.AbstractMap.SimpleEntry[String, AnyRef](START_NODE_KEY, properties.getValue(startNodeIndex)))
    set.add(new util.AbstractMap.SimpleEntry[String, AnyRef](END_NODE_KEY, properties.getValue(endNodeIndex)))
    set.add(new util.AbstractMap.SimpleEntry[String, AnyRef](PROPERTIES_KEY, propertyMap))
    set
  }

  override def containsKey(o: scala.Any): Boolean = throw new UnsupportedOperationException("Not Implemented")
  override def containsValue(o: scala.Any): Boolean = throw new UnsupportedOperationException("Not Implemented")
  override def get(o: scala.Any): AnyRef = throw new UnsupportedOperationException("Not Implemented")
  override def keySet(): util.Set[String] = throw new UnsupportedOperationException("Not Implemented")
  override def values(): util.Collection[AnyRef] = throw new UnsupportedOperationException("Not Implemented")

  override def put(k: String, v: AnyRef): AnyRef = throw new UnsupportedOperationException("Immutable")
  override def remove(o: scala.Any): AnyRef = throw new UnsupportedOperationException("Immutable")
  override def putAll(map: util.Map[_ <: String, _ <: AnyRef]): Unit = throw new UnsupportedOperationException("Immutable")
  override def clear(): Unit = throw new UnsupportedOperationException("Immutable")
}