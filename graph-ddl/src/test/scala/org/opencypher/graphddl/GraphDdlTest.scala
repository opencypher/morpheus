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
package org.opencypher.graphddl

import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTInteger
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

    val personKey1 = NodeViewKey(Set("Person"), QualifiedViewId("dataSourceName.fooDatabaseName.personView1"))
    val personKey2 = NodeViewKey(Set("Person"), QualifiedViewId("dataSourceName.fooDatabaseName.personView2"))
    val bookKey    = NodeViewKey(Set("Book"),   QualifiedViewId("dataSourceName.fooDatabaseName.bookView"))
    val readsKey1  = EdgeViewKey(Set("READS"),  QualifiedViewId("dataSourceName.fooDatabaseName.readsView1"))
    val readsKey2  = EdgeViewKey(Set("READS"),  QualifiedViewId("dataSourceName.fooDatabaseName.readsView2"))

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
              view = personKey1.qualifiedViewId,
              propertyMappings = Map("name" -> "person_name1", "age" -> "age")),
            personKey2 -> NodeToViewMapping(
              nodeType = Set("Person"),
              view = personKey2.qualifiedViewId,
              propertyMappings = Map("name" -> "person_name2", "age" -> "age")),
            bookKey -> NodeToViewMapping(
              nodeType = Set("Book"),
              view = bookKey.qualifiedViewId,
              propertyMappings = Map("title" -> "book_title"))
          ),
          List(
            EdgeToViewMapping(
              edgeType = Set("READS"),
              view = readsKey1.qualifiedViewId,
              startNode = StartNode(personKey1, List(Join("person_id1", "person"))),
              endNode = EndNode(bookKey, List(Join("book_id", "book"))),
              propertyMappings = Map("rating" -> "value1")),
            EdgeToViewMapping(
              edgeType = Set("READS"),
              view = readsKey2.qualifiedViewId,
              startNode = StartNode(personKey2, List(Join("person_id2", "person"))),
              endNode = EndNode(bookKey, List(Join("book_id", "book"))),
              propertyMappings = Map("rating" -> "value2"))
          )
        )
      )
    )

    graphDdl shouldEqual expected

  }

  it("fails on duplicate node mappings") {
    val e = the [GraphDdlException] thrownBy GraphDdl("""
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
    e.getFullMessage should (include("fooGraph") and include("Person") and include("personView"))
  }

  it("fails on duplicate global labels") {
    val e = the [GraphDdlException] thrownBy GraphDdl("""
      |CATALOG CREATE LABEL (Person)
      |CATALOG CREATE LABEL (Person)
    """.stripMargin)
    e.getFullMessage should include("Person")
  }

  it("fails on duplicate local labels") {
    val e = the [GraphDdlException] thrownBy GraphDdl("""
      |CREATE GRAPH SCHEMA fooSchema
      | LABEL (Person)
      | LABEL (Person)
    """.stripMargin)
    e.getFullMessage should (include("fooSchema") and include("Person"))
  }

  it("fails on duplicate graph types") {
    val e = the [GraphDdlException] thrownBy GraphDdl("""
      |CREATE GRAPH SCHEMA fooSchema
      |CREATE GRAPH SCHEMA fooSchema
    """.stripMargin)
    e.getFullMessage should include("fooSchema")
  }

  it("fails on duplicate graphs") {
    val e = the [GraphDdlException] thrownBy GraphDdl("""
      |CREATE GRAPH SCHEMA fooSchema
      |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
      |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
    """.stripMargin)
    e.getFullMessage should include("fooGraph")
  }

  it("fails on unresolved graph type") {
    val e = the [GraphDdlException] thrownBy GraphDdl("""
      |CREATE GRAPH SCHEMA fooSchema
      |CREATE GRAPH fooGraph WITH GRAPH SCHEMA barSchema
    """.stripMargin)
    e.getFullMessage should (include("fooGraph") and include("fooSchema") and include("barSchema"))
  }

  it("fails on unresolved labels") {
    val e = the [GraphDdlException] thrownBy GraphDdl("""
      |CREATE GRAPH SCHEMA fooSchema
      | LABEL (Person1)
      | LABEL (Person2)
      | (Person3, Person4)
    """.stripMargin)
    e.getFullMessage should (include("fooSchema") and include("(Person3,Person4)") and include("Person1") and include("Person2"))
  }

  it("fails on incompatible property types") {
    val e = the [GraphDdlException] thrownBy GraphDdl("""
      |CREATE GRAPH SCHEMA fooSchema
      | LABEL (Person1 { age: STRING  })
      | LABEL (Person2 { age: INTEGER })
      | (Person1, Person2)
    """.stripMargin)
    e.getFullMessage should (
      include("fooSchema") and include("(Person1,Person2)") and include("age") and include("STRING")  and include("INTEGER")
    )
  }

  it("fails on unresolved property names") {
    val e = the [GraphDdlException] thrownBy GraphDdl("""
      |SET SCHEMA a.b
      |CREATE GRAPH SCHEMA fooSchema
      | LABEL (Person { age1: STRING  })
      | (Person)
      |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
      |  NODE LABEL SETS (
      |    (Person) FROM personView ( person_name AS age2 )
      |  )
    """.stripMargin)
    e.getFullMessage should (
      include("fooGraph") and include("Person") and include("personView") and include("age1")  and include("age2")
    )
  }

  it("fails on missing set schema statement") {
    val e = the [GraphDdlException] thrownBy GraphDdl("""
      |CREATE GRAPH SCHEMA fooSchema
      | LABEL (Person)
      | (Person)
      |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
      |  NODE LABEL SETS (
      |    (Person) FROM personView
      |  )
    """.stripMargin)
    e.getFullMessage should (
      include("fooGraph") and include("personView")
    )
  }

}
