package org.opencypher.caps.test.support.testgraph

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.opencypher.caps.api.record.{NodeScan, RelationshipScan}
import org.opencypher.caps.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.spark.{CAPSRecords, CAPSScanGraph, CAPSSession}
import org.opencypher.caps.impl.convert.fromJavaType
import org.opencypher.caps.impl.spark.convert.toSparkType

import scala.collection.JavaConverters._

class TestGraphFactory(createQuery: String, externalParams: Map[String, Any] = Map.empty)
    (implicit session: CAPSSession) {

  val propertyGraph = CypherCreateParser(createQuery, externalParams)

  def schema: Schema = {
    def extractFromNode(n: Node) =
      n.labels -> n.properties.map {
        case (name, prop) => name -> fromJavaType(prop)
      }

    def extractFromRel(r: Relationship) =
      r.relType -> r.properties.map {
        case (name, prop) => name -> fromJavaType(prop)
      }

    val labelsAndProps = propertyGraph.nodes.map(extractFromNode)
    val typesAndProps = propertyGraph.relationships.map(extractFromRel)

    val schemaWithLabels = labelsAndProps.foldLeft(Schema.empty) {
      case (acc, (labels, props)) => acc.withNodePropertyKeys(labels, props)
    }

    typesAndProps.foldLeft(schemaWithLabels) {
      case (acc, (t, props)) => acc.withRelationshipPropertyKeys(t)(props.toSeq: _*)
    }
  }

  def asScanGraph: CAPSScanGraph = {
    val nodeScans = schema.labelCombinations.combos.map { labels =>
      val propKeys = schema.nodeKeys(labels)

      val idStructField = Seq(StructField("ID", LongType, nullable = false))
      val structType = StructType(idStructField ++ getPropertyStructFields(propKeys))

      val header = Seq("ID") ++ propKeys.keys
      val rows = propertyGraph.nodes
          .filter(_.labels == labels)
          .map { node =>
            val propertyValues = propKeys.map(key => node.properties(key._1))
            Row.fromSeq(Seq(node.id) ++ propertyValues)
          }

      val records = CAPSRecords.create(header: _*)(rows.asJava, structType)

      NodeScan.on("n" -> "ID")(_
          .build
          .withImpliedLabels(labels.toSeq: _*)
          .withPropertyKeys(propKeys.keys.toSeq: _*)
      ).from(records)
    }

    val relScans = schema.relationshipTypes.map { relType =>
      val propKeys = schema.relationshipKeys(relType)

      val idStructFields = Seq(
        StructField("ID", LongType, nullable = false),
        StructField("SRC", LongType, nullable = false),
        StructField("DST", LongType, nullable = false))
      val structType = StructType(idStructFields ++ getPropertyStructFields(propKeys))

      val header = Seq("ID", "SRC", "DST") ++ propKeys.keys
      val rows = propertyGraph.relationships
          .filter(_.relType == relType)
          .map { rel =>
            val propertyValues = propKeys.map(key => rel.properties(key._1))
            Row.fromSeq(Seq(rel.id, rel.startId, rel.endId) ++ propertyValues)
          }

      val records = CAPSRecords.create(header: _*)(rows.asJava, structType)

      RelationshipScan.on("r" -> "ID")(_
          .from("SRC")
          .to("DST")
          .relType(relType)
          .build
          .withPropertyKeys(propKeys.keys.toSeq: _*)
      ).from(records)
    }

    new CAPSScanGraph(nodeScans.toSeq ++ relScans, schema)
  }

  private def getPropertyStructFields(propKeys: PropertyKeys) = {
    propKeys.foldLeft(Seq.empty[StructField]) {
      case (fields, key) => fields :+ StructField(key._1, toSparkType(key._2), key._2.isNullable)
    }
  }
}
