package org.opencypher.spark.impl

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.SparkSession
import org.opencypher.spark._
import org.opencypher.spark.api.EntityId
import org.opencypher.spark.impl.newvalue.{CypherNode, CypherRelationship, CypherValue}

class StdPropertyGraphFactory(implicit private val session: SparkSession) extends PropertyGraphFactory {

  factory =>

  private val nodeIds = new AtomicLong(0L)
  private val relationshipIds = new AtomicLong(0L)

  val nodes = Seq.newBuilder[CypherNode]
  val relationships = Seq.newBuilder[CypherRelationship]

  private def sc = session.sqlContext

  override type Graph = StdPropertyGraph

  override def addNode(labels: Set[String], properties: Map[String, CypherValue]): CypherNode =
    nodeIds { id =>
      val node = CypherNode(id, labels.toSeq, properties)
      nodes += node
      node
    }

  override def addRelationship(startId: EntityId, relationshipType: String, endId: EntityId, properties: Map[String, CypherValue]): CypherRelationship =
    relationshipIds { id =>
      val relationship = CypherRelationship(id, startId, endId, relationshipType, properties)
      relationships += relationship
      relationship
    }

  override def graph: Graph = {
    import CypherValue.Encoders._

    val nodes = sc.createDataset(factory.nodes.result)
    val relationships = sc.createDataset(factory.relationships.result)

    new StdPropertyGraph(nodes, relationships)
  }

  override def clear(): Unit = {
    nodes.clear()
    relationships.clear()
    nodeIds.set(0)
    relationshipIds.set(0)
  }

  implicit class RichAtomicLong(pool: AtomicLong) {
    def apply[T](f: EntityId => T): T = {
      f(EntityId(pool.incrementAndGet()))
    }
  }
}

