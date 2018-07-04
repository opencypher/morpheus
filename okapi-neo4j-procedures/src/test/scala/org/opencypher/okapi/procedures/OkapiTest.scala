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
package org.opencypher.okapi.procedures

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.collection.JavaConverters._
import scala.collection.mutable

class OkapiTest extends FunSuite with BeforeAndAfter with Matchers {
  private var db: GraphDatabaseService = _

  before {
    db = new TestGraphDatabaseFactory()
      .newImpermanentDatabaseBuilder
      .setConfig(GraphDatabaseSettings.procedure_unrestricted, "*")
      .newGraphDatabase()

    registerProcedure(db, classOf[OkapiProcedures])
  }

  after {
    db.shutdown()
  }

  test("testOkapiSchemaForSingleLabel") {
    db.execute("CREATE (:A {val1: 'String', val2: 1})" + "CREATE (:A {val1: 'String', val2: 1.2})").close()
    testResult(db, "CALL org.opencypher.okapi.procedures.schema", (result) => {
      val expected = Set(
        Map(
          "type" -> "Node",
          "nodeLabelsOrRelType" -> Seq("A").asJava,
          "property" -> "val1",
          "cypherType" -> "STRING"
        ),
        Map(
          "type" -> "Node",
          "nodeLabelsOrRelType" -> Seq("A").asJava,
          "property" -> "val2",
          "cypherType" -> "NUMBER"
        )
      )
      result.toSet should equal(expected)
    })
  }

  test("testOkapiSchemaForSingleMultipleLabels") {
    db.execute("CREATE (:A {val1: 'String'})" + "CREATE (:B {val2: 2})" + "CREATE (:A:B {val1: 'String', val2: 2})").close()
    testResult(db, "CALL org.opencypher.okapi.procedures.schema", (result) => {
      val expected = Set(
        Map(
          "type" -> "Node",
          "nodeLabelsOrRelType" -> Seq("A").asJava,
          "property" -> "val1",
          "cypherType" -> "STRING"
        ),
        Map(
          "type" -> "Node",
          "nodeLabelsOrRelType" -> Seq("B").asJava,
          "property" -> "val2",
          "cypherType" -> "INTEGER"
        ),
        Map(
          "type" -> "Node",
          "nodeLabelsOrRelType" -> Seq("A", "B").asJava,
          "property" -> "val1",
          "cypherType" -> "STRING"
        ),
        Map(
          "type" -> "Node",
          "nodeLabelsOrRelType" -> Seq("A", "B").asJava,
          "property" -> "val2",
          "cypherType" -> "INTEGER"
        )
      )
      result.toSet should equal(expected)
    })
  }

  test("testOkapiSchemaForLabelWithEmptyLabel") {
    db.execute("CREATE ({val1: 'String'})").close()
    testResult(db, "CALL org.opencypher.okapi.procedures.schema", (result) => {
      val expected = Set(
        Map(
          "type" -> "Node",
          "nodeLabelsOrRelType" -> Seq.empty.asJava,
          "property" -> "val1",
          "cypherType" -> "STRING"
        )
      )
      result.toSet should equal(expected)
    })
  }

  test("testOkapiSchemaForLabelWithoutProperties") {
    db.execute("CREATE (:A)").close()
    testResult(db, "CALL org.opencypher.okapi.procedures.schema", (result) => {
      val expected = Set(
        Map(
          "type" -> "Node",
          "nodeLabelsOrRelType" -> Seq("A").asJava,
          "property" -> "",
          "cypherType" -> ""
        )
      )
      result.toSet should equal(expected)
    })
  }

  test("testOkapiSchemaForNullableProperty") {
    db.execute("CREATE (:A {val: 1}), (:A)").close()
    testResult(db, "CALL org.opencypher.okapi.procedures.schema", (result) => {
      val expected = Set(
        Map(
          "type" -> "Node",
          "nodeLabelsOrRelType" -> Seq("A").asJava,
          "property" -> "val",
          "cypherType" -> "INTEGER?"
        )
      )
      result.toSet should equal(expected)
    })
  }

  test("testOkapiSchemaForSingleRelationship") {
    db.execute("CREATE (a:A)" + "CREATE (b:A)" + "CREATE (a)-[:REL {val1: 'String', val2: true}]->(b)" + "CREATE (a)-[:REL {val1: 'String', val2: 2.0}]->(b)").close()
    testResult(db, "CALL org.opencypher.okapi.procedures.schema", (result) => {
      val expected = Set(
        Map(
          "type" -> "Node",
          "nodeLabelsOrRelType" -> Seq("A").asJava,
          "property" -> "",
          "cypherType" -> ""
        ),

        Map(
          "type" -> "Relationship",
          "nodeLabelsOrRelType" -> Seq("REL").asJava,
          "property" -> "val1",
          "cypherType" -> "STRING"
        ),

        Map(
          "type" -> "Relationship",
          "nodeLabelsOrRelType" -> Seq("REL").asJava,
          "property" -> "val2",
          "cypherType" -> "ANY"
        )
      )
      result.toSet should equal(expected)
    })
  }

  test("testOkapiSchemaForRelationshipWithoutProperties") {
    db.execute("CREATE (a:A)" + "CREATE (b:A)" + "CREATE (a)-[:REL]->(b)").close()
    testResult(db, "CALL org.opencypher.okapi.procedures.schema", (result) => {
      val expected = Set(
        Map(
          "type" -> "Node",
          "nodeLabelsOrRelType" -> Seq("A").asJava,
          "property" -> "",
          "cypherType" -> ""
        ),

        Map(
          "type" -> "Relationship",
          "nodeLabelsOrRelType" -> Seq("REL").asJava,
          "property" -> "",
          "cypherType" -> ""
        )
      )
      result.toSet should equal(expected)
    })
  }

  private def registerProcedure(db: GraphDatabaseService, procedures: Class[_]*): Unit = {
    val proceduresService = db.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[Procedures])
    for (procedure <- procedures) {
      proceduresService.registerProcedure(procedure, true)
      proceduresService.registerFunction(procedure, true)
      proceduresService.registerAggregationFunction(procedure, true)
    }
  }

  def testResult(
    db: GraphDatabaseService,
    call: String,
    consumer: Seq[mutable.Map[String, Object]] => Unit
  ): Unit = {
    val tx = db.beginTx
    try {
      consumer(db.execute(call).asScala.map(_.asScala).toSeq)
      tx.success()
    } finally if (tx != null) tx.close()
  }
}
