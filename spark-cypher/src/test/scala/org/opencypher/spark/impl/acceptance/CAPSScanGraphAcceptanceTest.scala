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
package org.opencypher.spark.impl.acceptance

import org.opencypher.spark.test.support.creation.caps.{CAPSScanGraphFactory, CAPSTestGraphFactory}

class CAPSScanGraphAcceptanceTest extends AcceptanceTest {
  override def capsGraphFactory: CAPSTestGraphFactory = CAPSScanGraphFactory

  it("foo") {

//    PrintFlatPlan.set
//    PrintPhysicalPlan.set
//    PrintOptimizedPhysicalPlan.set

    val graph = initGraph(
      """
        |CREATE (a {name: 'A'}), (b {name: 'B'}),
        |       (x1 {name: 'x1'})
        |CREATE (a)-[:KNOWS]->(x1),
        |       (b)-[:KNOWS]->(x1)
      """.stripMargin)

    val results = graph.cypher(
      """
        |MATCH (a {name: 'A'}), (b {name: 'B'})
        |MATCH (a)-[e]->(x)<-[f]->(b)
        |RETURN x
      """.stripMargin)

    results.show
  }


  it("should correctly perform a join after a cross") {
    val df1 = sparkSession.createDataFrame(Seq((0L, "A")))
      .toDF("a", "____a_dot_nameSTRING")
    val df2 = sparkSession.createDataFrame(Seq((1L, "B")))
      .toDF("b", "____b_dot_nameSTRING")
    val df3 = sparkSession.createDataFrame(Seq((1L, 4L, "KNOWS", 2L)))
      .toDF("____source(e)", "e", "____type(e)", "____target(e)")

    val cross = df1.crossJoin(df2)
    cross.show()
    val join = cross.join(df3, cross.col("b") === df3.col("____source(e)"))

    join.show()
  }
}
