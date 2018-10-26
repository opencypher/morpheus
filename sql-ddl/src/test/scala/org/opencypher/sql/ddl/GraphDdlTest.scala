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
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.api.schema.Schema
import org.scalatest.{Matchers, path}

class GraphDdlTest extends path.FunSpec with Matchers {


  it("converts to GraphDDL IR") {

    val ddlString =
      s"""
         |SET SCHEMA dataSourceName.fooDatabaseName
         |
         |CREATE GRAPH SCHEMA fooSchema
         | LABEL (Person { name   : STRING })
         | LABEL (Book   { title  : STRING })
         | LABEL (READS  { rating : FLOAT  })
         | (Person)
         | (Book)
         | [READS]
         |
         |CREATE GRAPH fooGraph WITH GRAPH SCHEMA fooSchema
         |  NODE LABEL SETS (
         |    (Person) FROM personView ( person_name AS name )
         |    (Book)   FROM bookView   ( book_title AS title )
         |  )
         |  RELATIONSHIP LABEL SETS (
         |    (READS)
         |      FROM readsView e ( value AS rating )
         |        START NODES
         |          LABEL SET (Person) FROM personView p JOIN ON p.person_id = e.person
         |        END NODES
         |          LABEL SET (Book)   FROM bookView   b JOIN ON e.book = b.book_id
         |  )
     """.stripMargin

    import org.opencypher.okapi.api.types.{CTFloat, CTString}

    val graphDdl = GraphDdl(ddlString)

    val personKey = NodeViewKey(Set("Person"), "personView")
    val bookKey = NodeViewKey(Set("Book"), "bookView")
    val readsKey = EdgeViewKey(Set("READS"), "readsView")

    val expected = GraphDdl(
      Map(
        GraphName("fooGraph") -> Graph(GraphName("fooGraph"),
          Schema.empty
            .withNodePropertyKeys("Person")("name" -> CTString)
            .withNodePropertyKeys("Book")("title" -> CTString)
            .withRelationshipPropertyKeys("READS")("rating" -> CTFloat),
          Map(
            personKey -> NodeToViewMapping(
              nodeType = Set("Person"),
              view = "personView",
              nodeMapping = NodeMapping
                .on("id")
                .withImpliedLabel("Person")
                .withPropertyKey("name" -> "person_name"),
              environment = DbEnv(DataSourceConfig())),
            bookKey -> NodeToViewMapping(
              nodeType = Set("Book"),
              view = "bookView",
              nodeMapping = NodeMapping
                .on("id")
                .withImpliedLabel("Book")
                .withPropertyKey("title" -> "book_title"),
              environment = DbEnv(DataSourceConfig()))
          ),
          Map(
            readsKey -> EdgeToViewMapping(
              edgeType = Set("READS"),
              view = "readsView",
              startNode = StartNode(personKey, List(Join("person_id", "person"))),
              endNode = EndNode(bookKey, List(Join("book_id", "book"))),
              relationshipMapping = RelationshipMapping
                .on("id")
                .withSourceStartNodeKey("start")
                .withSourceEndNodeKey("end").withRelType("READS")
                .withPropertyKey("rating" -> "value"),
              environment = DbEnv(DataSourceConfig())
            )
          )
        )
      )
    )

    graphDdl shouldEqual expected

  }

}
