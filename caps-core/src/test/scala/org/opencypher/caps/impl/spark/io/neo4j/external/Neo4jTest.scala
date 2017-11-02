/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.spark.io.neo4j.external

import org.junit.Assert.assertEquals
import org.opencypher.caps.test.BaseTestSuite
import org.opencypher.caps.test.fixture.{Neo4jServerFixture, SparkSessionFixture}

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

  lazy private val neo4j = Neo4j(neo4jConfig, session)

  test("run Cypher Query With Params") {
    val result = neo4j.cypher("MATCH (n:Person) WHERE n.id <= {maxId} RETURN id(n)").param("maxId", 10)
    assertEquals(10, result.loadRowRdd.count())
  }

  test("run Cypher Query") {
    val result = neo4j.cypher("MATCH (n:Person) RETURN id(n)")
    assertEquals(100, result.loadRowRdd.count())
  }

  test("run Cypher Query With Partition") {
    val result = neo4j.cypher("MATCH (n:Person) RETURN id(n) SKIP {_skip} LIMIT {_limit}").partitions(4).batch(25)
    assertEquals(100, result.loadRowRdd.count())
  }

  test("runCypherRelQueryWithPartition") {
    val result = neo4j.cypher("MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN id(n) as src,id(m) as dst,type(r) as value SKIP {_skip} LIMIT {_limit}").partitions(7).batch(200)
    assertEquals(1000, result.loadRowRdd.count())
  }
}
