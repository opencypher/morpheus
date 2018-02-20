package org.opencypher.caps.impl.spark.io.neo4j

import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.io.{GraphName, PropertyGraphDataSource}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.impl.spark.io.neo4j.external.Neo4jConfig

class Neo4jPropertyGraphDataSource(config: Neo4jConfig)(implicit val session: CAPSSession)
  extends PropertyGraphDataSource {

  override def graph(name: GraphName): PropertyGraph = Neo4jGraphLoader.fromNeo4j(config)

  override def schema(name: GraphName): Option[Schema] = None

  override def store(name: GraphName, graph: PropertyGraph): Unit = ???

  override def delete(name: GraphName): Unit = ???

  override def graphNames: Set[GraphName] = ???
}
