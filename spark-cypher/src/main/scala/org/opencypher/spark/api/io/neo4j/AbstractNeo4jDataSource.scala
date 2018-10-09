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
package org.opencypher.spark.api.io.neo4j

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.neo4j.io.{Neo4jConfig, SchemaFromProcedure}
import org.opencypher.spark.api.io.{AbstractPropertyGraphDataSource, Neo4jFormat, StorageFormat}
import org.opencypher.spark.api.io.metadata.CAPSGraphMetaData
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

abstract class AbstractNeo4jDataSource extends AbstractPropertyGraphDataSource {

  def config: Neo4jConfig

  def omitIncompatibleProperties = false

  override def tableStorageFormat: StorageFormat = Neo4jFormat

  override protected def readSchema(graphName: GraphName): CAPSSchema =
    SchemaFromProcedure(config, omitIncompatibleProperties) match {
      case None =>
        throw UnsupportedOperationException(
          "Neo4j PGDS requires okapi-neo4j-procedures to be installed in Neo4j:\n" +
            "\thttps://github.com/opencypher/cypher-for-apache-spark/wiki/Neo4j-Schema-Procedure")

      case Some(schema) =>
        schema.asCaps
  }

  override protected def writeSchema(graphName: GraphName, schema: CAPSSchema): Unit = ()
  override protected def readCAPSGraphMetaData(graphName: GraphName): CAPSGraphMetaData = CAPSGraphMetaData(tableStorageFormat.name)
  override protected def writeCAPSGraphMetaData(graphName: GraphName, capsGraphMetaData: CAPSGraphMetaData): Unit = ()
}
