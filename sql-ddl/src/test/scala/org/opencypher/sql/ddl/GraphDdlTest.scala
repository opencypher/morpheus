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
package org.opencypher.sql.ddl

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTInteger
import org.opencypher.okapi.impl.exception.SchemaException
import org.scalatest.{FunSpec, Matchers}

class GraphDdlTest extends FunSpec with Matchers {

  it("converts to GraphDDL IR") {

    val ddlString =
      s"""
         |SET SCHEMA dataSourceName.fooDatabaseName
         |
         |CREATE GRAPH SCHEMA fooSchema
         | LABEL (Person { name   : STRING, age : INTEGER })
         | LABEL (Book   { title  : STRING })
         | LABEL (READS  { rating : FLOAT  })
         | (Person)
         | (Book)
         | [READS]
         |
         |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
         |  NODE LABEL SETS (
         |    (Person) FROM personView1 ( person_name1 AS name )
         |             FROM personView2 ( person_name2 AS name )
         |    (Book)   FROM bookView    ( book_title AS title )
         |  )
         |  RELATIONSHIP LABEL SETS (
         |    (READS)
         |      FROM readsView1 e ( value1 AS rating )
         |        START NODES LABEL SET (Person) FROM personView1 p JOIN ON p.person_id1 = e.person
         |        END   NODES LABEL SET (Book)   FROM bookView    b JOIN ON e.book       = b.book_id
         |      FROM readsView2 e ( value2 AS rating )
         |        START NODES LABEL SET (Person) FROM personView2 p JOIN ON p.person_id2 = e.person
         |        END   NODES LABEL SET (Book)   FROM bookView    b JOIN ON e.book       = b.book_id
         |  )
     """.stripMargin

    import org.opencypher.okapi.api.types.{CTFloat, CTString}

    val graphDdl = GraphDdl(ddlString)

    val personKey1 = NodeViewKey(Set("Person"), "personView1")
    val personKey2 = NodeViewKey(Set("Person"), "personView2")
    val bookKey = NodeViewKey(Set("Book"), "bookView")
    val readsKey1 = EdgeViewKey(Set("READS"), "readsView1")
    val readsKey2 = EdgeViewKey(Set("READS"), "readsView2")

    val expected = GraphDdl(
      Map(
        GraphName("fooGraph") -> Graph(GraphName("fooGraph"),
          Schema.empty
            .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger)
            .withNodePropertyKeys("Book")("title" -> CTString)
            .withRelationshipPropertyKeys("READS")("rating" -> CTFloat),
          Map(
            personKey1 -> NodeToViewMapping(
              nodeType = Set("Person"),
              view = "personView1",
              propertyMappings = Map("name" -> "person_name1", "age" -> "age"),
              environment = DbEnv(DataSourceConfig())),
            personKey2 -> NodeToViewMapping(
              nodeType = Set("Person"),
              view = "personView2",
              propertyMappings = Map("name" -> "person_name2", "age" -> "age"),
              environment = DbEnv(DataSourceConfig())),
            bookKey -> NodeToViewMapping(
              nodeType = Set("Book"),
              view = "bookView",
              propertyMappings = Map("title" -> "book_title"),
              environment = DbEnv(DataSourceConfig()))
          ),
          Map(
            readsKey1 -> EdgeToViewMapping(
              edgeType = Set("READS"),
              view = "readsView1",
              startNode = StartNode(personKey1, List(Join("person_id1", "person"))),
              endNode = EndNode(bookKey, List(Join("book_id", "book"))),
              propertyMappings = Map("rating1" -> "value"),
              environment = DbEnv(DataSourceConfig())
            ),
            readsKey2 -> EdgeToViewMapping(
              edgeType = Set("READS"),
              view = "readsView2",
              startNode = StartNode(personKey2, List(Join("person_id2", "person"))),
              endNode = EndNode(bookKey, List(Join("book_id", "book"))),
              propertyMappings = Map("rating2" -> "value"),
              environment = DbEnv(DataSourceConfig())
            )
          )
        )
      )
    )

    graphDdl shouldEqual expected

  }

  it("fails on duplicate node mappings") {
    failsOn("""
      |CREATE GRAPH SCHEMA fooSchema
      | LABEL (Person)
      | (Person)
      |
      |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
      |  NODE LABEL SETS (
      |    (Person) FROM personView
      |             FROM personView
      |  )
    """.stripMargin)
  }

  it("fails on duplicate relationship mappings") {
    failsOn("""
      |CREATE GRAPH SCHEMA fooSchema
      | LABEL (Person)
      | LABEL (Book)
      | LABEL (READS)
      | (Person)
      | (Book)
      | [READS]
      |
      |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
      |  NODE LABEL SETS (
      |    (Person) FROM personView
      |    (Book)   FROM bookView
      |  )
      |  RELATIONSHIP LABEL SETS (
      |    (READS)
      |      FROM readsView e
      |        START NODES LABEL SET (Person) FROM personView p JOIN ON p.p = e.p
      |        END   NODES LABEL SET (Book)   FROM bookView   b JOIN ON e.b = b.b
      |      FROM readsView e
      |        START NODES LABEL SET (Person) FROM personView p JOIN ON p.p = e.p
      |        END   NODES LABEL SET (Book)   FROM bookView   b JOIN ON e.b = b.b
      |  )
    """.stripMargin)
  }

  it("fails on duplicate global labels") {
    failsOn("""
      |CATALOG CREATE LABEL (Person)
      |CATALOG CREATE LABEL (Person)
    """.stripMargin)
  }

  it("fails on duplicate local labels") {
    failsOn("""
      |CREATE GRAPH SCHEMA fooSchema
      | LABEL (Person)
      | LABEL (Person)
    """.stripMargin)
  }

  it("fails on duplicate graph types") {
    failsOn("""
      |CREATE GRAPH SCHEMA fooSchema
      |CREATE GRAPH SCHEMA fooSchema
    """.stripMargin)
  }

  it("fails on duplicate graphs") {
    failsOn("""
      |CREATE GRAPH SCHEMA fooSchema
      |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
      |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
    """.stripMargin)
  }


  private def failsOn(ddl: String) =
    a[SchemaException] should be thrownBy GraphDdl(ddl)

}
