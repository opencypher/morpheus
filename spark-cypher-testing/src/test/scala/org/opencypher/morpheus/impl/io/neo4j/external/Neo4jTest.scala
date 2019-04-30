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
package org.opencypher.morpheus.impl.io.neo4j.external

import org.junit.Assert.assertEquals
import org.opencypher.morpheus.testing.fixture.SparkSessionFixture
import org.opencypher.okapi.neo4j.io.testing.Neo4jServerFixture
import org.opencypher.okapi.testing.BaseTestSuite

class Neo4jTest extends BaseTestSuite
  with SparkSessionFixture
  with Neo4jServerFixture {

  override def dataFixture: String =
    """
      UNWIND range(1,100) as id
      CREATE (p:Person {id:id}) WITH collect(p) as people
      UNWIND people as p1
      UNWIND range(1,10) as friend
      WITH p1, people[(p1.id + friend) % size(people)] as p2
      CREATE (p1)-[:KNOWS]->(p2)
      """

  lazy private val neo4j = Neo4j(neo4jConfig, sparkSession)

  test("run Cypher Query With Params") {
    val result = neo4j.cypher("MATCH (n:Person) WHERE n.id <= {maxId} RETURN id(n)").param("maxId", 10)
    assertEquals(10, result.loadRowRdd.count())
  }

  test("run Cypher Node Query") {
    val result = neo4j.cypher("MATCH (n:Person) RETURN id(n)")
    assertEquals(100, result.loadRowRdd.count())
  }

  test("run Cypher Rel Query") {
    val result = neo4j.cypher("MATCH ()-[r:KNOWS]->() RETURN id(r)")
    assertEquals(1000, result.loadRowRdd.count())
  }

  test("run Cypher Query With Partition") {
    val result = neo4j.cypher("MATCH (n:Person) RETURN id(n) SKIP {_skip} LIMIT {_limit}").partitions(4).batch(25)
    assertEquals(100, result.loadRowRdd.count())
  }

  test("run Cypher Rel Query WithPartition") {
    val result = neo4j.cypher("MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN id(n) as src,id(m) as dst,type(r) as value SKIP {_skip} LIMIT {_limit}").partitions(7).batch(200)
    assertEquals(1000, result.loadRowRdd.count())
  }
}
