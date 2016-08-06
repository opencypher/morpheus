package org.opencypher.spark.impl

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.SparkSession
import org.opencypher.spark._

class StdPropertyGraphFactory(implicit private val session: SparkSession) extends PropertyGraphFactory {

  factory =>

  private val nodeIds = new AtomicLong(0L)
  private val relationshipIds = new AtomicLong(0L)

  private val nodes = Seq.newBuilder[CypherNode]
  private val relationships = Seq.newBuilder[CypherRelationship]

  private def sc = session.sqlContext

  override def addNode(labels: Set[String], properties: Map[String, CypherValue]): EntityId =
    nodeIds { id => nodes += CypherNode(id, labels.toSeq, properties) }

  override def addRelationship(startId: EntityId, relationshipType: String, endId: EntityId, properties: Map[String, CypherValue]): EntityId =
    relationshipIds { id => relationships += CypherRelationship(id, startId, endId, relationshipType, properties) }

  override def graph: StdPropertyGraph =
    new StdPropertyGraph {
      import CypherValue.implicits._

      val nodes = sc.createDataset(factory.nodes.result)
      val relationships = sc.createDataset(factory.relationships.result)
    }

  override def clear(): Unit = {
    nodes.clear()
    relationships.clear()
    nodeIds.set(0)
    relationshipIds.set(0)
  }

  implicit class RichAtomicLong(pool: AtomicLong) {
    def apply(f: EntityId => Unit): EntityId = {
      val id = EntityId(pool.incrementAndGet())
      f(id)
      id
    }
  }
}

