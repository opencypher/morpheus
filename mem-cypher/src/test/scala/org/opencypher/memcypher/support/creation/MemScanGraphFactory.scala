package org.opencypher.memcypher.support.creation

import org.opencypher.memcypher.api.MemCypherSession
import org.opencypher.memcypher.impl.cyphertable.{MemNodeTable, MemRelationshipTable}
import org.opencypher.memcypher.impl.table.{ColumnSchema, Row, Schema, Table}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types.CTInteger
import org.opencypher.okapi.relational.impl.graph.ScanGraph
import org.opencypher.okapi.testing.propertygraph.{CypherTestGraphFactory, InMemoryTestGraph}
import org.opencypher.spark.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.spark.testing.support.creation.caps.CAPSScanGraphFactory.tableEntityIdKey

object MemScanGraphFactory extends CypherTestGraphFactory[MemCypherSession] {
  override def name: String = "MemScanGraphFactory"

  override def apply(propertyGraph: InMemoryTestGraph)(implicit session: MemCypherSession): ScanGraph[Table] = {
    val graphSchema = computeSchema(propertyGraph)

    val nodeScans = graphSchema.labelCombinations.combos.map { labels =>
      val propKeys = graphSchema.nodePropertyKeys(labels)

      val idColumnSchema = Array(ColumnSchema(tableEntityIdKey, CTInteger))
      val tableSchema = Schema(idColumnSchema ++ getPropertyColumnSchemas(propKeys))

      val rows = propertyGraph.nodes
        .filter(_.labels == labels)
        .map { node =>
          val propertyValues = propKeys.map(key =>
            node.properties.unwrap.getOrElse(key._1, null)
          )
          Row.fromSeq(Seq(node.id) ++ propertyValues)
        }

      MemNodeTable.fromMapping(NodeMapping
        .on(tableEntityIdKey)
        .withImpliedLabels(labels.toSeq: _*)
        .withPropertyKeys(propKeys.keys.toSeq: _*), Table(tableSchema, rows))
    }

    val relScans = graphSchema.relationshipTypes.map { relType =>
      val propKeys = graphSchema.relationshipPropertyKeys(relType)

      val idColumnSchemas = Array(
        ColumnSchema(tableEntityIdKey, CTInteger),
        ColumnSchema(sourceStartNodeKey, CTInteger),
        ColumnSchema(sourceEndNodeKey, CTInteger))
      val tableSchema = Schema(idColumnSchemas ++ getPropertyColumnSchemas(propKeys))

      val rows = propertyGraph.relationships
        .filter(_.relType == relType)
        .map { rel =>
          val propertyValues = propKeys.map(key => rel.properties.unwrap.getOrElse(key._1, null))
          Row.fromSeq(Seq(rel.id, rel.startId, rel.endId) ++ propertyValues)
        }

      MemRelationshipTable.fromMapping(RelationshipMapping
        .on(tableEntityIdKey)
        .from(sourceStartNodeKey)
        .to(sourceEndNodeKey)
        .relType(relType)
        .withPropertyKeys(propKeys.keys.toSeq: _*), Table(tableSchema, rows))
    }

    new ScanGraph(nodeScans.toSeq ++ relScans, graphSchema, Set(0))
  }

  protected def getPropertyColumnSchemas(propKeys: PropertyKeys): Seq[ColumnSchema] = {
    propKeys.foldLeft(Seq.empty[ColumnSchema]) {
      case (fields, key) => fields :+ ColumnSchema(key._1, key._2)
    }
  }
}
