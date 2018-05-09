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
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.testing.propertygraph.InMemoryTestGraph
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.api.io.CAPSPGDSAcceptance
import org.opencypher.spark.testing.fixture.Neo4jServerFixture

class Neo4jPGDSAcceptanceTest extends CAPSTestSuite with Neo4jServerFixture with CAPSPGDSAcceptance {

  override def initSession(): CAPSSession = caps

  override def create(graphName: GraphName, testGraph: InMemoryTestGraph, createStatements: String): PropertyGraphDataSource = {
    new CommunityNeo4jGraphDataSource(neo4jConfig, Map(graphName -> CommunityNeo4jGraphDataSource.defaultQuery))
  }

  override def dataFixture: String = createStatements
}
