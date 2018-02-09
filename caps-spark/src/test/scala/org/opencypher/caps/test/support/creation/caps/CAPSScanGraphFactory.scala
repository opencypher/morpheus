/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencypher.caps.test.support.creation.caps

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.opencypher.caps.api.CAPSSession
import org.opencypher.caps.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.caps.api.schema.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.caps.impl.spark.{CAPSGraph, CAPSScanGraph}
import org.opencypher.caps.test.support.creation.propertygraph.PropertyGraph

import scala.collection.JavaConverters._

object CAPSScanGraphFactory extends CAPSTestGraphFactory {


  override def apply(propertyGraph: PropertyGraph)(implicit caps: CAPSSession): CAPSGraph = {
    val schema = computeSchema(propertyGraph)

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

      val records = caps.sparkSession.createDataFrame(rows.asJava, structType).toDF(header: _*)

      CAPSNodeTable(NodeMapping
        .on("ID")
        .withImpliedLabels(labels.toSeq: _*)
        .withPropertyKeys(propKeys.keys.toSeq: _*), records)
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

      val records = caps.sparkSession.createDataFrame(rows.asJava, structType).toDF(header: _*)

      CAPSRelationshipTable(RelationshipMapping
        .on("ID")
        .from("SRC")
        .to("DST")
        .relType(relType)
        .withPropertyKeys(propKeys.keys.toSeq: _*), records)
    }

    new CAPSScanGraph(nodeScans.toSeq ++ relScans, schema)
  }

  override def name: String = "CAPSScanGraphFactory"
}
