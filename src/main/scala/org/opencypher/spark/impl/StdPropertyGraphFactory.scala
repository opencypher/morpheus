package org.opencypher.spark.impl

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.opencypher.spark._

class StdPropertyGraphFactory(sc: SQLContext) extends PropertyGraphFactory {

  factory =>

  private val nodeIds = new AtomicLong(0L)
  private val relationshipIds = new AtomicLong(0L)

  private val nodes = Seq.newBuilder[CypherNode]
  private val relationships = Seq.newBuilder[CypherRelationship]

  override def addNode(properties: Map[String, CypherValue]): EntityId =
    nodeIds { id => nodes += CypherNode(id, Seq.empty, properties) }

  override def addLabeledNode(labels: String*)(properties: Map[String, CypherValue]): EntityId =
    nodeIds { id => nodes += CypherNode(id, labels, properties) }

  override def addRelationship(start: EntityId, end: EntityId, typ: String, properties: Map[String, CypherValue]): EntityId =
    relationshipIds { id => relationships += CypherRelationship(id, start, end, typ, properties) }

  override def reset(): Unit = {
    nodes.clear()
    relationships.clear()
    nodeIds.set(0)
    relationshipIds.set(0)
  }

  override def result: StdPropertyGraph =
    new StdPropertyGraph(sc) {
      import CypherValue.implicits._

      val nodes = sc.createDataset(factory.nodes.result)
      val relationships = sc.createDataset(factory.relationships.result)
    }

  implicit class RichAtomicLong(pool: AtomicLong) {
    def apply(f: EntityId => Unit): EntityId = {
      val id = EntityId(pool.incrementAndGet())
      f(id)
      id
    }
  }
}
