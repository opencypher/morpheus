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
package org.opencypher.okapi.neo4j.io.testing

import org.opencypher.okapi.neo4j.io.Neo4jConfig
import org.opencypher.okapi.neo4j.io.testing.Neo4jTestUtils.Neo4jContext
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.testcontainers.neo4j.Neo4jContainer

trait Neo4jServerFixture extends BeforeAndAfterAll {
  self: Suite =>

  def dataFixture: String

  var neo4jContext: Neo4jContext = _
  var neo4jContainer: Neo4jContainer = _

  def boltUrl: String = neo4jContainer.getBoltUrl
  def neo4jConfig: Neo4jConfig = neo4jContext.config

  abstract override def beforeAll(): Unit = {
    super.beforeAll()
    // Temporary solution to remove obsolete spawn gradle plugin.
    // If we want to keep this it is probably a good idea to make it a fixture
    // that expose the neo4j logs on failures in some way.
    neo4jContainer = new Neo4jContainer("neo4j:3.4.10-enterprise")
    neo4jContainer = neo4jContainer.withExposedPorts(7687)
    neo4jContainer.start()

    neo4jContext = Neo4jTestUtils.connectNeo4j(dataFixture, neo4jContainer.getBoltUrl)
  }

  abstract override def afterAll(): Unit = {
    try if (neo4jContext != null) neo4jContext.close()
    finally {
      try if (neo4jContainer != null) neo4jContainer.stop()
      finally super.afterAll()
    }
  }
}
