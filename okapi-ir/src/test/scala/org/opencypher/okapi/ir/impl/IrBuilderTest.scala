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
package org.opencypher.okapi.ir.impl

import org.opencypher.okapi.api.graph.{GraphName, Namespace, QualifiedGraphName}
import org.opencypher.okapi.api.schema.{PropertyKeys, PropertyGraphSchema}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.block._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.impl.exception.ParsingException
import org.opencypher.okapi.ir.impl.typer.UnTypedExpr
import org.opencypher.okapi.ir.impl.util.VarConverters._
import org.opencypher.okapi.testing.MatchHelper.equalWithTracing

import scala.collection.immutable.Set

class IrBuilderTest extends IrTestSuite {

  describe("CONSTRUCT") {
    it("sets the correct type for new elements") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(qgn, _, _, news, _, _)) =>
          news.fields.size should equal(1)
          val a = news.fields.head
          a.cypherType.graph should equal(Some(qgn))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes the correct schema for new elements") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a:A {name:'Hans'})-[rel:KNOWS {since:2007}]->(a)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema shouldEqual PropertyGraphSchema.empty
            .withNodePropertyKeys("A")("name" -> CTString)
            .withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger)
        case _ => fail("no matching graph result found")
      }
    }

    it("sets the correct type for clone aliases") {
      val query =
        """
          |MATCH (a)
          |CONSTRUCT
          |  CLONE a as b
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(qgn, _, clones, _, _, _)) =>
          clones.keys.size should equal(1)
          val (b, a) = clones.head
          a should equal(NodeVar("a")())
          a.asInstanceOf[Var].cypherType.graph should equal(Some(testGraph.qualifiedGraphName))
          b.cypherType.graph should equal(Some(qgn))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 1 create") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a :A)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A")())
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 2 creates") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a:A)
          |  CREATE (b:B:C)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A")().withNodePropertyKeys("B", "C")())
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - setting2 labels") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a:A:D)
          |  CREATE (b:B:C)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A", "D")().withNodePropertyKeys("B", "C")())
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - setting 3 labels") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a:A:B:C)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A", "B", "C")())
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - setting 2 different label combinations with overlap") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a:A:B)
          |  CREATE (b:A:C)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A", "B")().withNodePropertyKeys("A", "C")())
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - setting 2 equal label combinations") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a:A:B)
          |  CREATE (b:B:A)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A", "B")())
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - setting a property") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a:A {name : 'Mats'})
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A")("name" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - setting a node property and a label combination") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a:A:B {name : 'Mats'})
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A", "B")("name" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 1  rel property set") {
      val query =
        """
          |CONSTRUCT
          |  CREATE ()-[r:R {level : 'high'}]->()
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys(Set.empty[String]).withRelationshipPropertyKeys("R")("level" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  2  properties set") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a:A {category : 'computer', ports : 4})
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys(Set("A"), PropertyKeys("category" -> CTString, "ports" -> CTInteger)))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied nodes") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH (a: A)
          |CONSTRUCT
          |  CREATE (COPY OF a)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(inputSchema)
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied nodes with unspecified labels") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)
        .withNodePropertyKeys("B")("category" -> CTString, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH (a)
          |CONSTRUCT
          |  CREATE (COPY OF a)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(inputSchema)
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied nodes with additional Label") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH (a: A)
          |CONSTRUCT
          |  CREATE (b COPY OF a:B)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys("A", "B")("category" -> CTString, "ports" -> CTInteger))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied unspecified nodes with additional Label") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)
        .withNodePropertyKeys("B")("foo" -> CTString, "bar" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH (a)
          |CONSTRUCT
          |  CREATE (b COPY OF a:C)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys("A", "C")("category" -> CTString, "ports" -> CTInteger)
            .withNodePropertyKeys("B", "C")("foo" -> CTString, "bar" -> CTInteger)
          )
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied nodes with additional properties") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH (a: A)
          |CONSTRUCT
          |  CREATE (b COPY OF a {memory: "1TB"})
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys("A")("category" -> CTString, "ports" -> CTInteger, "memory" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied nodes with conflicting properties") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH (a: A)
          |CONSTRUCT
          |  CREATE (b COPY OF a {category: 0})
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys("A")("category" -> CTInteger, "ports" -> CTInteger))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied unspecified nodes with conflicting properties") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)
        .withNodePropertyKeys("B")("category" -> CTInteger, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH (a)
          |CONSTRUCT
          |  CREATE (b COPY OF a {category: 0})
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys("A")("category" -> CTInteger, "ports" -> CTInteger)
            .withNodePropertyKeys("B")("category" -> CTInteger, "ports" -> CTInteger))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied relationships") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH ()-[a:A]->()
          |CONSTRUCT
          |  CREATE ()-[COPY OF a]->()
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(inputSchema)
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied relationships with unspecified type") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)
        .withRelationshipPropertyKeys("B")("category" -> CTString, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH ()-[r]->()
          |CONSTRUCT
          |  CREATE ()-[r2 COPY OF r]->()
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(inputSchema)
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied relationships with alternative types") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)
        .withRelationshipPropertyKeys("B")("category" -> CTString, "ports" -> CTInteger)
        .withRelationshipPropertyKeys("C")("foo" -> CTString, "bar" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH ()-[r:A|B]->()
          |CONSTRUCT
          |  CREATE ()-[r2 COPY OF r]->()
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys()()
            .withRelationshipPropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)
            .withRelationshipPropertyKeys("B")("category" -> CTString, "ports" -> CTInteger)
          )
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied relationships with different type") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH ()-[a:A]->()
          |CONSTRUCT
          |  CREATE ()-[b COPY OF a:B]->()
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys()()
            .withRelationshipPropertyKeys("B")("category" -> CTString, "ports" -> CTInteger)
          )
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied relationships with unspecified types and different type") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("A")("category" -> CTString)
        .withRelationshipPropertyKeys("B")("ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH ()-[r]->()
          |CONSTRUCT
          |  CREATE ()-[r2 COPY OF r :C]->()
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys()()
            .withRelationshipPropertyKeys("C")("category" -> CTString.nullable, "ports" -> CTInteger.nullable)
          )
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied relationships with additional properties") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH ()-[a:A]->()
          |CONSTRUCT
          |  CREATE ()-[b COPY OF a {memory: "1TB"}]->()
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys()()
            .withRelationshipPropertyKeys("A")("category" -> CTString, "ports" -> CTInteger, "memory" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied relationships with conflicting properties") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH ()-[a:A]->()
          |CONSTRUCT
          |  CREATE ()-[b COPY OF a {category: 2}]->()
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys()()
            .withRelationshipPropertyKeys("A")("category" -> CTInteger, "ports" -> CTInteger))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied unspecified relationships with conflicting properties") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)
        .withRelationshipPropertyKeys("B")("category" -> CTInteger, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH ()-[a]->()
          |CONSTRUCT
          |  CREATE ()-[b COPY OF a {category: 2}]->()
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys()()
            .withRelationshipPropertyKeys("A")("category" -> CTInteger, "ports" -> CTInteger)
            .withRelationshipPropertyKeys("B")("category" -> CTInteger, "ports" -> CTInteger))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied relationships with unspecified types, different type and updated properties") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("A")("category" -> CTString)
        .withRelationshipPropertyKeys("B")("ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH ()-[r]->()
          |CONSTRUCT
          |  CREATE ()-[r2 COPY OF r :C {memory: "1TB"}]->()
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys()()
            .withRelationshipPropertyKeys("C")("category" -> CTString.nullable, "ports" -> CTInteger.nullable, "memory" -> CTString)
          )
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly -  for copied relationships with alternative types and additional property") {

      val graphName = GraphName("input")
      val inputSchema = PropertyGraphSchema.empty
        .withNodePropertyKeys()()
        .withRelationshipPropertyKeys("A")("category" -> CTString, "ports" -> CTInteger)
        .withRelationshipPropertyKeys("B")("category" -> CTString, "ports" -> CTInteger)

      val query =
        """
          |FROM GRAPH testNamespace.input
          |MATCH ()-[r:A|B]->()
          |CONSTRUCT
          |  CREATE ()-[r2 COPY OF r {memory: "1TB"}]->()
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery(graphName -> inputSchema).model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys()()
            .withRelationshipPropertyKeys("A")("category" -> CTString, "ports" -> CTInteger, "memory" -> CTString)
            .withRelationshipPropertyKeys("B")("category" -> CTString, "ports" -> CTInteger, "memory" -> CTString)
          )
        case _ => fail("no matching graph result found")
      }
    }

    it("throws an error when a relationships is cloned that is not part of a new pattern") {
      val query =
        """
          |MATCH ()-[r]->()
          |CONSTRUCT
          | CLONE r AS r
          |RETURN GRAPH
        """.stripMargin

      intercept[UnsupportedOperationException](query.asCypherQuery().model)
    }

    it("allows cloning relationships with aliased newly constructed start and end nodes") {
      val query =
        """
          |MATCH (:FOO)-[r:REL]->()
          |CONSTRUCT
          | CLONE r AS newR
          | CREATE (:A)-[newR]->()
          |RETURN GRAPH
        """.stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys("A")()
            .withNodePropertyKeys()()
            .withRelationshipPropertyKeys("REL")())
        case _ => fail("no matching graph result found")
      }
    }

    it("allows cloning relationships with newly constructed start and end nodes") {
      val query =
        """
          |MATCH (:FOO)-[r:REL]->()
          |CONSTRUCT
          |  CLONE r AS r
          |  CREATE (:A)-[r]->()
          |RETURN GRAPH
        """.stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys("A")()
            .withNodePropertyKeys()()
            .withRelationshipPropertyKeys("REL")())
        case _ => fail("no matching graph result found")
      }
    }

    it("allows implicit cloning of relationships with newly constructed start and end nodes") {
      val query =
        """
          |MATCH (:FOO)-[r:REL]->()
          |CONSTRUCT
          |  CREATE (:A)-[r]->()
          |RETURN GRAPH
        """.stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty
            .withNodePropertyKeys("A")()
            .withNodePropertyKeys()()
            .withRelationshipPropertyKeys("REL")())
        case _ => fail("no matching graph result found")
      }
    }
  }

  describe("parsing CypherQuery") {
    test("match node and return it") {
      "MATCH (a:Person) RETURN a".asCypherQuery().model.ensureThat {
        (model, _) =>
          val loadBlock = model.findExactlyOne {
            case NoWhereBlock(s@SourceBlock(_)) =>
              s.binds.fields shouldBe empty
          }

          val matchBlock = model.findExactlyOne {
            case MatchBlock(deps, Pattern(fields, topo, _, _), exprs, _, _) =>
              deps should equalWithTracing(List(loadBlock))
              fields should equal(Set(toField('a -> CTNode("Person"))))
              topo shouldBe empty
              exprs should equalWithTracing(Set.empty)
          }

          val projectBlock = model.findExactlyOne {
            case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) =>
              deps should equalWithTracing(List(matchBlock))
              map should equal(Map(toField('a) -> toNodeVar('a)))
          }

          model.result match {
            case NoWhereBlock(TableResultBlock(deps, OrderedFields(List(IRField("a"))), _)) =>
              deps should equal(List(projectBlock))
          }

          model.dependencies should equalWithTracing(
            Set(matchBlock, loadBlock, projectBlock, model.result)
          )
      }
    }


    it("matches a simple relationship pattern and returns some fields") {
      "MATCH (a)-[r]->(b) RETURN b AS otherB, a, r".asCypherQuery().model.ensureThat {
        (model, globals) =>
          val loadBlock = model.findExactlyOne {
            case NoWhereBlock(s@SourceBlock(_)) =>
              s.binds.fields shouldBe empty
          }

          val matchBlock = model.findExactlyOne {
            case NoWhereBlock(MatchBlock(deps, Pattern(fields, topo, _, _), _, _, _)) =>
              deps should equalWithTracing(List(loadBlock))
              fields should equal(Set[IRField]('a -> CTNode, 'b -> CTNode, 'r -> CTRelationship))
              val map = Map(toField('r) -> DirectedRelationship('a, 'b))
              topo should equal(map)
          }

          val projectBlock = model.findExactlyOne {
            case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) =>
              deps should equalWithTracing(List(matchBlock))
              map should equal(
                Map(
                  toField('a) -> toNodeVar('a),
                  toField('otherB) -> toNodeVar('b),
                  toField('r) -> toRelVar('r)
                ))
          }

          val resultBlock = model.result.findExactlyOne {
            case TableResultBlock(_, OrderedFields(List(IRField("otherB"), IRField("a"), IRField("r"))), _) =>
          }

          model.dependencies should equalWithTracing(
            Set(matchBlock, loadBlock, projectBlock, resultBlock)
          )
      }
    }

    it("matches node order by name and returns it") {
      "FROM GRAPH foo MATCH (a:Person) WITH a.name AS name, a.age AS age ORDER BY age RETURN age, name"
        .asCypherQuery(GraphName("foo") -> PropertyGraphSchema.empty
          .withNodePropertyKeys("Person")(
            "name" -> CTString,
            "age" -> CTInteger
          )).model.ensureThat {
        (model, _) =>
          val loadBlock = model.findExactlyOne {
            case NoWhereBlock(s@SourceBlock(_)) =>
              s.binds.fields shouldBe empty
          }

          val matchBlock = model.findExactlyOne {
            case MatchBlock(deps, Pattern(fields, topo, _, _), exprs, _, _) =>
              deps should equalWithTracing(List(loadBlock))
              fields should equal(Set(toField('a -> CTNode("Person"))))
              topo shouldBe empty
              exprs should equalWithTracing(Set.empty)
          }

          val projectBlock1 = model.findExactlyOne {
            case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) if deps.head == matchBlock =>
              deps should equalWithTracing(List(matchBlock))
              map should equal(
                Map(
                  toField('name) -> ElementProperty(Var("a")(CTNode), PropertyKey("name"))(CTString),
                  toField('age) -> ElementProperty(Var("a")(CTNode), PropertyKey("age"))(CTInteger)
                ))
          }

          val projectBlock2 = model.findExactlyOne {
            case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) if deps.head == projectBlock1 =>
              deps should equalWithTracing(List(projectBlock1))
              map should equal(
                Map(
                  toField('age) -> toVar('age),
                  toField('name) -> toVar('name)
                ))
          }

          val orderByBlock = model.findExactlyOne {
            case NoWhereBlock(OrderAndSliceBlock(deps, orderBy, None, None, _)) =>
              val ordered = List(Asc(toVar('age)))
              orderBy should equal(ordered)
              deps should equalWithTracing(List(projectBlock2))
          }

          val projectBlock3 = model.findExactlyOne {
            case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) if deps.head == orderByBlock =>
              deps should equalWithTracing(List(orderByBlock))
              map should equal(
                Map(
                  toField('age) -> toVar('age),
                  toField('name) -> toVar('name)
                ))
          }

          val resultBlock = model.findExactlyOne {
            case TableResultBlock(deps, OrderedFields(List(IRField("age"), IRField("name"))), _) =>
              deps should equalWithTracing(List(projectBlock3))
          }

          model.dependencies should equalWithTracing(
            Set(orderByBlock, projectBlock3, projectBlock2, projectBlock1, matchBlock, loadBlock, resultBlock)
          )
      }
    }
  }

  describe("CreateGraphStatement") {
    it("can parse a CATALOG CREATE GRAPH statement") {
      val innerQuery = s"FROM GRAPH ${
        testQualifiedGraphName.toString
      } RETURN GRAPH"

      val query =
        s"""
           |CATALOG CREATE GRAPH session.bar {
           | $innerQuery
           |}
        """.stripMargin

      val result = query.parseIR[CreateGraphStatement]()

      result.innerQuery.model should equalWithTracing(innerQuery.asCypherQuery().model)
      result.graph.qualifiedGraphName should equal(QualifiedGraphName(Namespace("session"), GraphName("bar")))
      result.graph.schema should equal(testGraphSchema)
    }
  }

  describe("DropGraphStatement/DropViewStatement") {
    it("can parse a DROP GRAPH statement") {
      val query = s"CATALOG DROP GRAPH $testQualifiedGraphName"

      val result = query.parseIR[DeleteGraphStatement]()

      result.qgn should equal(testQualifiedGraphName)
    }

    it("can parse a DROP VIEW statement") {
      val query = s"CATALOG DROP VIEW $testQualifiedGraphName"

      val result = query.parseIR[DeleteViewStatement]()

      result.qgn should equal(testQualifiedGraphName)
    }
  }

  describe("SET in construct") {

    it("fails when setting a label on an element that is not in scope") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a)
          |CONSTRUCT
          |  SET a :Label
          |RETURN GRAPH""".stripMargin

      intercept[UnTypedExpr](query.asCypherQuery().model)
    }

    it("fails when setting a label on a relationship that is out of scope") {
      val query =
        """
          |CONSTRUCT
          |  CREATE ()-[r:KNOWS]->()
          |CONSTRUCT
          |  SET r :Label
          |RETURN GRAPH""".stripMargin

      intercept[ParsingException](query.asCypherQuery().model)
    }

    it("computes a pattern graph schema correctly - 2 creates") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a :A)
          |  CREATE (b :B:C)
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A")().withNodePropertyKeys("B", "C")())
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 2 creates and a set") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a :A)
          |  CREATE (b :B:C)
          |  SET a :D
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A", "D")().withNodePropertyKeys("B", "C")())
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 1 create and 2 sets") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a :A)
          |  SET a :B
          |  SET a :C
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A", "B", "C")())
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 2 creates and 2 sets on same label") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a :A)
          |  CREATE (b :A)
          |  SET a :B
          |  SET b :C
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A", "B")().withNodePropertyKeys("A", "C")())
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 2 creates and 2 sets on different labels") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a :A)
          |  CREATE (b :B)
          |  SET a :B
          |  SET b :A
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A", "B")())
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 1 create and 1 set property") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a :A)
          |  SET a.name = 'Mats'
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A")("name" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 1 create and 1 set property with two labels") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a :A:B)
          |  SET a.name = 'Mats'
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A", "B")("name" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 1 create and 1 set rel property") {
      val query =
        """
          |CONSTRUCT
          |  CREATE ()-[r :R]->()
          |  SET r.level = 'high'
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys(Set.empty[String]).withRelationshipPropertyKeys("R")("level" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 1 create and 2 set properties") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a :A)
          |  SET a.category = 'computer'
          |  SET a.ports = 4
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys(Set("A"), PropertyKeys("category" -> CTString, "ports" -> CTInteger)))
        case _ => fail("no matching graph result found")
      }
    }

    it("computes a pattern graph schema correctly - 1 create from equivalent") {
      val query =
        """
          |CONSTRUCT
          |  CREATE (a :A)
          |MATCH (b: A)
          |CONSTRUCT
          |  CLONE b as c
          |  SET c :B
          |RETURN GRAPH""".stripMargin

      query.asCypherQuery().model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(PropertyGraphSchema.empty.withNodePropertyKeys("A", "B")())
        case _ => fail("no matching graph result found")
      }
    }
  }

  implicit class RichBlock(b: Block) {

    def findExactlyOne(f: PartialFunction[Block, Unit]): Block = {
      val results = b.collect {
        case block if f.isDefinedAt(block) =>
          f(block)
          block
      }
      withClue(s"Failed to extract single matching block from $b") {
        results.size should equal(1)
      }
      results.head
    }
  }

  implicit class RichModel(model: QueryModel) {

    def ensureThat(f: (QueryModel, CypherMap) => Unit): Unit = f(model, model.parameters)

  }

}
