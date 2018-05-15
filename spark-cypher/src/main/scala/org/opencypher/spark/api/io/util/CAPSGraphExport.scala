package org.opencypher.spark.api.io.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.ColumnName
import org.opencypher.spark.api.io.{GraphEntity, Relationship}
import org.opencypher.spark.impl.CAPSGraph

object CAPSGraphExport {

  // TODO: Add prefixes for labels/properties to avoid collisions, ensure Cypher names are appropriately encoded/decoded for Spark column name compatibility.
  implicit class CanonicalTableSparkSchema(val schema: Schema) extends AnyVal {

    import org.opencypher.spark.impl.convert.CAPSCypherType._

    def canonicalNodeTableSchema(labels: Set[String]): StructType = {
      val id = StructField(GraphEntity.sourceIdKey, LongType, nullable = false)
      val properties = schema.nodeKeys(labels).toSeq.sortBy(_._1).map { case (propertyName, cypherType) =>
        StructField(propertyName, cypherType.getSparkType, cypherType.isNullable)
      }
      StructType(id +: properties)
    }

    def canonicalRelTableSchema(relType: String): StructType = {
      val id = StructField(GraphEntity.sourceIdKey, LongType, nullable = false)
      val sourceId = StructField(Relationship.sourceStartNodeKey, LongType, nullable = false)
      val targetId = StructField(Relationship.sourceEndNodeKey, LongType, nullable = false)
      val properties = schema.relationshipKeys(relType).toSeq.sortBy(_._1).map { case (propertyName, cypherType) =>
        StructField(propertyName, cypherType.getSparkType, cypherType.isNullable)
      }
      StructType(id +: sourceId +: targetId +: properties)
    }
  }

  implicit class CanonicalTableExport(graph: CAPSGraph) {

    def canonicalNodeTable(labels: Set[String]): DataFrame = {
      val varName = "n"
      val nodeRecords = graph.nodesWithExactLabels(varName, labels)

      val idRenaming = varName -> GraphEntity.sourceIdKey
      val propertyRenamings = nodeRecords.header.propertySlots(Var(varName)())
        .map { case (p, slot) => ColumnName.of(slot) -> p.key.name }

      val selectColumns = (idRenaming :: propertyRenamings.toList.sorted).map {
        case (oldName, newName) => nodeRecords.data.col(oldName).as(newName)
      }

      nodeRecords.data.select(selectColumns: _*)
    }

    def canonicalRelationshipTable(relType: String): DataFrame = {
      val varName = "r"
      val relCypherType = CTRelationship(relType)
      val v = Var(varName)(relCypherType)

      val relRecords = graph.relationships(varName, relCypherType)

      val idRenaming = varName -> GraphEntity.sourceIdKey
      val sourceIdRenaming = ColumnName.of(relRecords.header.sourceNodeSlot(v)) -> Relationship.sourceStartNodeKey
      val targetIdRenaming = ColumnName.of(relRecords.header.targetNodeSlot(v)) -> Relationship.sourceEndNodeKey
      val propertyRenamings = relRecords.header.propertySlots(Var(varName)())
        .map { case (p, slot) => ColumnName.of(slot) -> p.key.name }

      val selectColumns = (idRenaming :: sourceIdRenaming :: targetIdRenaming :: propertyRenamings.toList.sorted).map {
        case (oldName, newName) => relRecords.data.col(oldName).as(newName)
      }

      relRecords.data.select(selectColumns: _*)
    }

  }

}
