package org.opencypher.morpheus.adapters

import org.apache.spark.graph.api._
import org.apache.spark.sql.DataFrame
import org.opencypher.morpheus.MorpheusCypherSession
import org.opencypher.morpheus.adapters.MappingAdapter._
import org.opencypher.morpheus.api.io.MorpheusElementTable
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr.Var

case class RelationalGraphAdapter(
  cypherSession: MorpheusCypherSession,
  nodeFrames: Seq[NodeDataset],
  relationshipFrames: Seq[RelationshipDataset]) extends PropertyGraph {

  override def schema: PropertyGraphSchema = SchemaAdapter(graph.schema)

  private[morpheus] lazy val graph = {
    if (nodeFrames.isEmpty) {
      cypherSession.graphs.empty
    } else {
      val nodeTables = nodeFrames.map { nodeDataFrame => MorpheusElementTable.create(nodeDataFrame.toNodeMapping, nodeDataFrame.ds) }
      val relTables = relationshipFrames.map { relDataFrame => MorpheusElementTable.create(relDataFrame.toRelationshipMapping, relDataFrame.ds) }
      cypherSession.graphs.create(nodeTables.head, nodeTables.tail ++ relTables: _*)
    }
  }

  private lazy val _nodeFrame: Map[Set[String], NodeDataset] = nodeFrames.map(nf => nf.labelSet -> nf).toMap

  private lazy val _relationshipFrame: Map[String, RelationshipDataset] = relationshipFrames.map(rf => rf.relationshipType -> rf).toMap

  override def nodes: DataFrame = {
    // TODO: move to API as default implementation
    val nodeVar = Var("n")(CTNode)
    val nodes = graph.nodes(nodeVar.name)

    val df = nodes.table.df
    val header = nodes.header

    val idRename = header.column(nodeVar) -> "$ID"
    val labelRenames = header.labelsFor(nodeVar).map(hasLabel => header.column(hasLabel) -> s":${hasLabel.label.name}").toSeq.sortBy(_._2)
    val propertyRenames = header.propertiesFor(nodeVar).map(property => header.column(property) -> property.key.name).toSeq.sortBy(_._2)

    val selectColumns = (Seq(idRename) ++ labelRenames ++ propertyRenames).map { case (oldColumn, newColumn) => df.col(oldColumn).as(newColumn) }

    df.select(selectColumns: _*)
  }

  override def relationships: DataFrame = {
    // TODO: move to API as default implementation
    val relVar = Var("r")(CTRelationship)
    val rels = graph.relationships(relVar.name)

    val df = rels.table.df
    val header = rels.header

    val idRename = header.column(relVar) -> "$ID"
    val sourceIdRename = header.column(header.startNodeFor(relVar)) -> "$SOURCE_ID"
    val targetIdRename = header.column(header.endNodeFor(relVar)) -> "$TARGET_ID"
    val relTypeRenames = header.typesFor(relVar).map(hasType => header.column(hasType) -> s":${hasType.relType.name}").toSeq.sortBy(_._2)
    val propertyRenames = header.propertiesFor(relVar).map(property => header.column(property) -> property.key.name).toSeq.sortBy(_._2)

    val selectColumns = (Seq(idRename, sourceIdRename, targetIdRename) ++ relTypeRenames ++ propertyRenames).map { case (oldColumn, newColumn) => df.col(oldColumn).as(newColumn) }

    df.select(selectColumns: _*)
  }

  override def nodeDataset(labelSet: Array[String]): NodeDataset = _nodeFrame(labelSet.toSet)

  override def relationshipDataset(relationshipType: String): RelationshipDataset = _relationshipFrame(relationshipType)

  override def write: PropertyGraphWriter = ???
}
