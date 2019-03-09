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
package org.opencypher.spark.api.io.sql

import org.opencypher.graphddl._
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.api.types.{CTFloat, CTInteger, CTString}
import org.opencypher.okapi.testing.BaseTestSuite
import GraphDdlConversions._

class GraphDdlConversionsTest extends BaseTestSuite {

  describe("convert GraphType to OKAPI schema") {
    it("converts a graph type with single element type references") {
      GraphDdl(
        """
          |CREATE GRAPH myGraph (
          | Person ( name STRING, age INTEGER ),
          | Book   ( title STRING ) ,
          | READS  ( rating FLOAT ) ,
          | (Person),
          | (Book),
          | (Person)-[READS]->(Book)
          |)
        """.stripMargin).graphs(GraphName("myGraph")).graphType.asOkapiSchema should equal(Schema.empty
        .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger)
        .withNodePropertyKeys("Book")("title" -> CTString)
        .withRelationshipPropertyKeys("READS")("rating" -> CTFloat)
        .withSchemaPatterns(SchemaPattern("Person", "READS", "Book")))
    }

    it("converts a graph type with multiple element type references") {
      GraphDdl(
        """
          |CREATE GRAPH myGraph (
          |  A (x STRING),
          |  B (y STRING),
          |  R (y STRING),
          |  (A),
          |  (A, B),
          |  (A)-[R]->(A),
          |  (A, B)-[R]->(A)
          |)
        """.stripMargin).graphs(GraphName("myGraph")).graphType.asOkapiSchema should equal(Schema.empty
        .withNodePropertyKeys("A")("x" -> CTString)
        .withNodePropertyKeys("A", "B")("x" -> CTString, "y" -> CTString)
        .withRelationshipPropertyKeys("R")("y" -> CTString)
        .withSchemaPatterns(SchemaPattern("A", "R", "A"))
        .withSchemaPatterns(SchemaPattern(Set("A", "B"), "R", Set("A"))))
    }

    it("converts a graph type with element type inheritance") {
      GraphDdl(
        """
          |CREATE GRAPH myGraph (
          |  A           (x STRING),
          |  B EXTENDS A (y STRING),
          |  R (y STRING),
          |  (A),
          |  (B),
          |  (A)-[R]->(A),
          |  (B)-[R]->(A)
          |)
        """.stripMargin).graphs(GraphName("myGraph")).graphType.asOkapiSchema should equal(Schema.empty
        .withNodePropertyKeys("A")("x" -> CTString)
        .withNodePropertyKeys("A", "B")("x" -> CTString, "y" -> CTString)
        .withRelationshipPropertyKeys("R")("y" -> CTString)
        .withSchemaPatterns(SchemaPattern("A", "R", "A"))
        .withSchemaPatterns(SchemaPattern(Set("A", "B"), "R", Set("A"))))
    }

  }
}
