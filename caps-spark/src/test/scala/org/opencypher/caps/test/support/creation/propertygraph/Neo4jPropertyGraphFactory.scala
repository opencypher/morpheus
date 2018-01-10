package org.opencypher.caps.test.support.creation.propertygraph

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.harness.TestServerBuilders

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

class Neo4jPropertyGraphFactory extends PropertyGraphFactory {

  private val neo4jServer = TestServerBuilders.newInProcessBuilder()
    .withConfig("dbms.security.auth_enabled", "true")
    .newServer()

  val inputGraph: GraphDatabaseService = neo4jServer.graph()

  override def create(createQuery: String, parameters: Map[String, Any]): PropertyGraph = {
    val tx = inputGraph.beginTx()
    inputGraph.execute("MATCH (a) DETACH DELETE a")
    inputGraph.execute(createQuery)

    val nodes = inputGraph.getAllNodes.iterator().asScala.map { neoNode =>
      val labels: Set[String] = neoNode.getLabels.asScala.map(_.name).toSet
      val id: Long = neoNode.getId
      val properties: Map[String, Any] = neoNode.getAllProperties.asScala.toMap

      Node(id, labels, properties)
    }.toSeq

    val relationships = inputGraph.getAllRelationships.iterator().asScala.map { neoRel =>
      val relType: String = neoRel.getType.name
      val sourceId: Long = neoRel.getStartNodeId
      val targetId: Long = neoRel.getEndNodeId
      val id: Long = neoRel.getId
      val properties: Map[String, Any] = neoRel.getAllProperties.asScala.toMap

      Relationship(id, sourceId, targetId, relType, properties)
    }.toSeq

    tx.success()

    PropertyGraph(nodes, relationships)
  }

  def close: Any = neo4jServer.close()
}
