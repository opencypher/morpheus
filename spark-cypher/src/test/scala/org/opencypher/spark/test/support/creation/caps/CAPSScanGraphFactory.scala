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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.test.support.creation.caps

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.ir.test.support.creation.propertygraph.TestPropertyGraph
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.convert.CAPSCypherType._
import org.opencypher.spark.impl.{CAPSGraph, CAPSScanGraph}
import org.opencypher.spark.schema.CAPSSchema._

import scala.collection.JavaConverters._

object CAPSScanGraphFactory extends CAPSTestGraphFactory {

  override def apply(propertyGraph: TestPropertyGraph)(implicit caps: CAPSSession): CAPSGraph = {
    val schema = computeSchema(propertyGraph).asCaps

    val nodeScans = schema.labelCombinations.combos.map { labels =>
      val propKeys = schema.nodeKeys(labels)

      val idStructField = Seq(StructField("ID", StringType, nullable = false))
      val structType = StructType(idStructField ++ getPropertyStructFields(propKeys))

      val header = Seq("ID") ++ propKeys.keys
      val rows = propertyGraph.nodes
        .filter(_.labels == labels)
        .map { node =>
          val propertyValues = propKeys.map(key =>
            node.properties.unwrap.getOrElse(key._1, null)
          )
          Row.fromSeq(Seq(node.id.toString) ++ propertyValues)
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
        StructField("ID", StringType, nullable = false),
        StructField("SRC", StringType, nullable = false),
        StructField("DST", StringType, nullable = false))
      val structType = StructType(idStructFields ++ getPropertyStructFields(propKeys))

      val header = Seq("ID", "SRC", "DST") ++ propKeys.keys
      val rows = propertyGraph.relationships
        .filter(_.relType == relType)
        .map { rel =>
          val propertyValues = propKeys.map(key => rel.properties.unwrap.getOrElse(key._1, null))
          Row.fromSeq(Seq(rel.id.toString, rel.source.toString, rel.target.toString) ++ propertyValues)
        }

      val records = caps.sparkSession.createDataFrame(rows.asJava, structType).toDF(header: _*)

      CAPSRelationshipTable(RelationshipMapping
        .on("ID")
        .from("SRC")
        .to("DST")
        .relType(relType)
        .withPropertyKeys(propKeys.keys.toSeq: _*), records)
    }

    new CAPSScanGraph(nodeScans.toSeq ++ relScans, schema, Set(0))
  }

  override def name: String = "CAPSScanGraphFactory"

  protected def getPropertyStructFields(propKeys: PropertyKeys): Seq[StructField] = {
    propKeys.foldLeft(Seq.empty[StructField]) {
      case (fields, key) => fields :+ StructField(key._1, key._2.getSparkType, key._2.isNullable)
    }
  }
}
