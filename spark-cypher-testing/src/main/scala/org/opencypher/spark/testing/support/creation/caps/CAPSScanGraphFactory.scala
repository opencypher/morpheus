/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.testing.support.creation.caps

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.relational.impl.graph.ScanGraph
import org.opencypher.okapi.testing.propertygraph.InMemoryTestGraph
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.GraphEntity.sourceIdKey
import org.opencypher.spark.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.convert.SparkConversions._
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable
import org.opencypher.spark.schema.CAPSSchema._

import scala.collection.JavaConverters._

object CAPSScanGraphFactory extends CAPSTestGraphFactory {

  val tableEntityIdKey = s"___$sourceIdKey"

  override def apply(propertyGraph: InMemoryTestGraph)(implicit caps: CAPSSession): ScanGraph[DataFrameTable] = {
    val schema = computeSchema(propertyGraph).asCaps

    val nodeScans = schema.labelCombinations.combos.map { labels =>
      val propKeys = schema.nodePropertyKeys(labels)

      val idStructField = Seq(StructField(tableEntityIdKey, LongType, nullable = false))
      val structType = StructType(idStructField ++ getPropertyStructFields(propKeys))

      val header = Seq(tableEntityIdKey) ++ propKeys.keys
      val rows = propertyGraph.nodes
        .filter(_.labels == labels)
        .map { node =>
          val propertyValues = propKeys.map(key =>
            node.properties.unwrap.getOrElse(key._1, null)
          )
          Row.fromSeq(Seq(node.id) ++ propertyValues)
        }

      val records = caps.sparkSession.createDataFrame(rows.asJava, structType).toDF(header: _*)

      CAPSNodeTable.fromMapping(NodeMapping
        .on(tableEntityIdKey)
        .withImpliedLabels(labels.toSeq: _*)
        .withPropertyKeys(propKeys.keys.toSeq: _*), records)
    }

    val relScans = schema.relationshipTypes.map { relType =>
      val propKeys = schema.relationshipPropertyKeys(relType)

      val idStructFields = Seq(
        StructField(tableEntityIdKey, LongType, nullable = false),
        StructField(sourceStartNodeKey, LongType, nullable = false),
        StructField(sourceEndNodeKey, LongType, nullable = false))
      val structType = StructType(idStructFields ++ getPropertyStructFields(propKeys))

      val header = Seq(tableEntityIdKey, sourceStartNodeKey, sourceEndNodeKey) ++ propKeys.keys
      val rows = propertyGraph.relationships
        .filter(_.relType == relType)
        .map { rel =>
          val propertyValues = propKeys.map(key => rel.properties.unwrap.getOrElse(key._1, null))
          Row.fromSeq(Seq(rel.id, rel.startId, rel.endId) ++ propertyValues)
        }

      val records = caps.sparkSession.createDataFrame(rows.asJava, structType).toDF(header: _*)

      CAPSRelationshipTable.fromMapping(RelationshipMapping
        .on(tableEntityIdKey)
        .from(sourceStartNodeKey)
        .to(sourceEndNodeKey)
        .relType(relType)
        .withPropertyKeys(propKeys.keys.toSeq: _*), records)
    }

    new ScanGraph(nodeScans.toSeq ++ relScans, schema, Set(0))
  }

  override def name: String = "CAPSScanGraphFactory"

  protected def getPropertyStructFields(propKeys: PropertyKeys): Seq[StructField] = {
    propKeys.foldLeft(Seq.empty[StructField]) {
      case (fields, key) => fields :+ StructField(key._1, key._2.getSparkType, key._2.isNullable)
    }
  }
}
