package org.opencypher.caps.test.support.creation.caps

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.opencypher.caps.api.record.{NodeScan, RelationshipScan}
import org.opencypher.caps.api.spark.{CAPSGraph, CAPSRecords, CAPSScanGraph, CAPSSession}
import org.opencypher.caps.test.support.creation.propertygraph.PropertyGraph

import scala.collection.JavaConverters._

case class CAPSScanGraphFactory(propertyGraph: PropertyGraph)(implicit caps: CAPSSession) extends CAPSGraphFactory {
  override lazy val graph: CAPSGraph = {
    val nodeScans = schema.labelCombinations.combos.map { labels =>
      val propKeys = schema.nodeKeys(labels)

      val idStructField = Seq(StructField("ID", LongType, nullable = false))
      val structType = StructType(idStructField ++ getPropertyStructFields(propKeys))

      val header = Seq("ID") ++ propKeys.keys
      val rows = propertyGraph.nodes
        .filter(_.labels == labels)
        .map { node =>
          val propertyValues = propKeys.map(key =>
            node.properties.getOrElse(key._1, null)
          )
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
          val propertyValues = propKeys.map(key => rel.properties.getOrElse(key._1, null))
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
}
