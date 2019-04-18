package org.opencypher.morpheus

import org.apache.spark.graph.api._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.opencypher.morpheus.adapters.RelationalGraphAdapter
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.table.RelationalCypherRecords
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.CAPSEntityTableFactory
import org.opencypher.spark.impl.CAPSRecordsFactory
import org.opencypher.spark.impl.graph.CAPSGraphFactory
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable

object MorpheusSession {
  def create(implicit spark: SparkSession): MorpheusSession = new MorpheusSession(spark)

  implicit class CypherSessionOps(val cypherSession: CypherSession) extends AnyVal {
    def withCypher10: MorpheusSession = cypherSession match {
      case ms: MorpheusSession => ms
      case other => ???
    }
  }
}

case class SparkCypherResult(relationalTable: RelationalCypherRecords[DataFrameTable]) extends CypherResult {
  override val df: DataFrame = relationalTable.table.df
}

private[morpheus] class MorpheusSession(override val sparkSession: SparkSession) extends RelationalCypherSession[DataFrameTable] with CypherSession {

  implicit val caps: CAPSSession = new CAPSSession(sparkSession)

  override type Records = caps.Records

  override val records: CAPSRecordsFactory = caps.records

  override val graphs: CAPSGraphFactory = caps.graphs

  override val entityTables: CAPSEntityTableFactory.type = caps.entityTables

  // org.apache.spark.graph.api.CypherSession

  override def cypher(
    graph: PropertyGraph,
    query: String
  ): CypherResult = cypher(graph, query, Map.empty)

  override def cypher(
    graph: PropertyGraph,
    query: String,
    parameters: Map[String, Any]
  ): CypherResult = {
    val relationalGraph = toRelationalGraph(graph)
    SparkCypherResult(relationalGraph.cypher(query, CypherMap(parameters.toSeq: _*)).records)

  }

  override def createGraph(
    nodes: Seq[NodeFrame],
    relationships: Seq[RelationshipFrame]
  ): PropertyGraph = {
    require(nodes.groupBy(_.labelSet).forall(_._2.size == 1),
      "There can be at most one NodeFrame per label set")
    require(relationships.groupBy(_.relationshipType).forall(_._2.size == 1),
      "There can be at most one RelationshipFrame per relationship type")
    RelationalGraphAdapter(this, nodes, relationships)
  }

  override def load(path: String): PropertyGraph = ???

  override def save(
    graph: PropertyGraph,
    path: String,
    saveMode: SaveMode
  ): Unit = ???

  private def toRelationalGraph(graph: PropertyGraph): RelationalCypherGraph[DataFrameTable] = {
    graph match {
      case adapter: RelationalGraphAdapter => adapter.graph
      case other => throw IllegalArgumentException(
        expected = "A graph that has been created by `SparkCypherSession.createGraph`",
        actual = other.getClass.getSimpleName
      )
    }
  }
}




