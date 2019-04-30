/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.api.io.json

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.spark.api.io.AbstractPropertyGraphDataSource
import org.opencypher.spark.api.io.metadata.MorpheusGraphMetaData
import org.opencypher.spark.schema.MorpheusSchema

trait JsonSerialization {
  self: AbstractPropertyGraphDataSource =>

  import MorpheusSchema._

  protected def readJsonSchema(graphName: GraphName): String

  protected def writeJsonSchema(graphName: GraphName, schema: String): Unit

  protected def readJsonMorpheusGraphMetaData(graphName: GraphName): String

  protected def writeJsonMorpheusGraphMetaData(graphName: GraphName, morpheusGraphMetaData: String): Unit

  override protected[io] def readSchema(graphName: GraphName): MorpheusSchema = {
    PropertyGraphSchema.fromJson(readJsonSchema(graphName)).asMorpheus
  }

  override protected def writeSchema(graphName: GraphName, schema: MorpheusSchema): Unit = {
    writeJsonSchema(graphName, schema.schema.toJson)
  }

  override protected def readMorpheusGraphMetaData(graphName: GraphName): MorpheusGraphMetaData = {
    MorpheusGraphMetaData.fromJson(readJsonMorpheusGraphMetaData(graphName))
  }

  override protected def writeMorpheusGraphMetaData(graphName: GraphName, morpheusGraphMetaData: MorpheusGraphMetaData): Unit = {
    writeJsonMorpheusGraphMetaData(graphName, morpheusGraphMetaData.toJson)
  }

}
