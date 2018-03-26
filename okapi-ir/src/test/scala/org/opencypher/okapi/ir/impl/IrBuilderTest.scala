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
package org.opencypher.okapi.ir.impl

import org.opencypher.okapi.api.graph.{GraphName, Namespace, QualifiedGraphName}
import org.opencypher.okapi.api.schema.{PropertyKeys, Schema}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.block._
import org.opencypher.okapi.ir.api.expr.{Expr, HasLabel, Property, Var}
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.impl.exception.ParsingException
import org.opencypher.okapi.ir.test._
import org.opencypher.okapi.ir.test.support.MatchHelper.equalWithTracing

import scala.collection.immutable.Set

class IrBuilderTest extends IrTestSuite {

  describe("parsing CypherQuery") {it("computes a pattern graph schema correctly - 1 create") {
    val query =
      """
        |CONSTRUCT
        |  NEW (a :A)

        |RETURN GRAPH""".stripMargin

      query.asCypherQuery.model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A")())
        case _ => fail("no matching graph result found")
      }
    }

  // TODO: Enable again once setting in NEW is supported
  ignore("construct can seta label ") {
    val query =
      """
        |CONSTRUCT
        |  NEW ( a :Label
        )
        |RETURN GRAPH""".stripMargin

      intercept[UnsupportedOperationException](query.asCypherQuery.model)
    }

  // TODO: Enable again once setting in NEW is supported
  ignore("construct can set a newrelationshiptype") {
    val query =
      """
        |CONSTRUCT
        |  NEW ()-[ r :Label
        ]->()
        |RETURN GRAPH""".stripMargin

      intercept[ParsingException](query.asCypherQuery.model)
    }

  it("computes a pattern graph schema correctly - 2 creates") {
    val query =
      """
        |CONSTRUCT
        |  NEW (a :A)
        |  NEW (b :B:C)

        |RETURN GRAPH""".stripMargin

      query.asCypherQuery.model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A")().withNodePropertyKeys("B", "C")())
        case _ => fail("no matching graph result found")
      }
    }

  it("computes a pattern graph schema correctly - setting2 labels") {
    val query =
      """
        |CONSTRUCT
        |  NEW (a :A:D)
        |  NEW (b :B:C)

        |RETURN GRAPH""".stripMargin

      query.asCypherQuery.model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "D")().withNodePropertyKeys("B", "C")())
        case _ => fail("no matching graph result found")
      }
    }

  it("computes a pattern graph schema correctly - setting 3 labels") {
    val query =
      """
        |CONSTRUCT
        |  NEW (a :A :B
         :C
        )
        |RETURN GRAPH""".stripMargin

      query.asCypherQuery.model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "B", "C")())
        case _ => fail("no matching graph result found")
      }
    }

  it("computes a pattern graph schema correctly - setting 2 different labelcombinations with overlap") {
    val query =
      """
        |CONSTRUCT
        |  NEW (a :A :B)
        |  NEW ( b :A:C
        )
        |RETURN GRAPH""".stripMargin

      query.asCypherQuery.model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "B")().withNodePropertyKeys("A", "C")())
        case _ => fail("no matching graph result found")
      }
    }

  it("computes a pattern graph schema correctly - setting 2 equal label combinations") {
    val query =
      """
        |CONSTRUCT
        |  NEW (a :A :B)
        |  NEW ( b :B:A
        )
        |RETURN GRAPH""".stripMargin

      query.asCypherQuery.model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "B")())
        case _ => fail("no matching graph result found")
      }
    }

  // TODO: Enable again once setting in NEW is supported
  ignore("computes a pattern graph schema correctly - setting a property") {
    val query =
      """
        |CONSTRUCT
        |  NEW (a :A{name : 'Mats'
        })
        |RETURN GRAPH""".stripMargin

      query.asCypherQuery.model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A")("name" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }

  // TODO: Enable again once setting in NEW is supported
  ignore("computes a pattern graph schema correctly - setting a node property and a label combination") {
    val query =
      """
        |CONSTRUCT
        |  NEW (a :A:B{name : 'Mats'
        })
        |RETURN GRAPH""".stripMargin

      query.asCypherQuery.model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "B")("name" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }

  // TODO: Enable again once setting in NEW is supported
  ignore("computes a pattern graph schema correctly - 1  rel propertyset") {
    val query =
      """
        |CONSTRUCT
        |  NEW ()-[r :R{level : 'high'
        }]->()
        |RETURN GRAPH""".stripMargin

      query.asCypherQuery.model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys(Set.empty[String]).withRelationshipPropertyKeys("R")("level" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }

  // TODO: Enable again once setting in NEW is supported
  ignore("computes a pattern graph schema correctly -  2  propertiesset") {
    val query =
      """
        |CONSTRUCT
        |  CREATE (a :A{category : 'computer'
        ,ports : 4
        })
        |RETURN GRAPH""".stripMargin

      query.asCypherQuery.model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys(Set("A"), PropertyKeys("category" -> CTString, "ports" -> CTInteger)))
        case _ => fail("no matching graph result found")
      }
    }

  // TODO: Enable again once setting in NEW is  supported
  ignore("computes a pattern graph schema correctly - clone then add label") {
    val query =
      """
        |CONSTRUCT
        |  NEW (a :A)

        |MATCH (b: A)
        |CONSTRUCT
        |  CLONE b as c
        |  NEW ( c :B
        )
        |RETURN GRAPH""".stripMargin

      query.asCypherQuery.model.result match {
        case GraphResultBlock(_, IRPatternGraph(_, schema, _, _, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "B")())
        case _ => fail("no matching graph result found")
      }
    }

    test("match node and return it") {
      "MATCH (a:Person) RETURN a".asCypherQuery.model.ensureThat { (model, globals) =>
        val loadBlock = model.findExactlyOne {
          case NoWhereBlock(s@SourceBlock(_)) =>
            s.binds.fields shouldBe empty
        }

        val matchBlock = model.findExactlyOne {
          case MatchBlock(deps, Pattern(fields, topo, equivalences), exprs, _, _) =>
            deps should equalWithTracing(List(loadBlock))
            fields should equal(Set(toField('a -> CTNode)))
            topo shouldBe empty
            exprs should equalWithTracing(Set(HasLabel(toVar('a), Label("Person"))()))
        }

        val projectBlock = model.findExactlyOne {
          case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) =>
            deps should equalWithTracing(List(matchBlock))
            map should equal(Map(toField('a) -> toVar('a)))
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
      "MATCH (a)-[r]->(b) RETURN b AS otherB, a, r".asCypherQuery.model.ensureThat { (model, globals) =>
        val loadBlock = model.findExactlyOne {
          case NoWhereBlock(s@SourceBlock(_)) =>
            s.binds.fields shouldBe empty
        }

        val matchBlock = model.findExactlyOne {
          case NoWhereBlock(MatchBlock(deps, Pattern(fields, topo, equivalences), _, _, _)) =>
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
                toField('a) -> toVar('a),
                toField('otherB) -> toVar('b),
                toField('r) -> toVar('r)
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
      "MATCH (a:Person) WITH a.name AS name, a.age AS age ORDER BY age RETURN age, name".asCypherQuery.model.ensureThat {
        (model, _) =>
          val loadBlock = model.findExactlyOne {
            case NoWhereBlock(s@SourceBlock(_)) =>
              s.binds.fields shouldBe empty
          }

          val matchBlock = model.findExactlyOne {
            case MatchBlock(deps, Pattern(fields, topo, equivalences), exprs, _, _) =>
              deps should equalWithTracing(List(loadBlock))
              fields should equal(Set(toField('a -> CTNode)))
              topo shouldBe empty
              exprs should equalWithTracing(Set(HasLabel(toVar('a), Label("Person"))()))
          }

          val projectBlock1 = model.findExactlyOne {
            case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) if deps.head == matchBlock =>
              deps should equalWithTracing(List(matchBlock))
              map should equal(
                Map(
                  toField('name) -> Property(Var("a")(CTNode), PropertyKey("name"))(CTNull),
                  toField('age) -> Property(Var("a")(CTNode), PropertyKey("age"))(CTNull)
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

    //  ignore("can handle return graph of") {
    //    "MATCH (a), (b) RETURN GRAPH moo OF (a)-[r:TEST]->(b)".asCypherQuery.model.ensureThat { (model, globals) =>
    //      val expectedSchema = Schema.empty
    //        .withNodePropertyKeys(Set.empty[String], PropertyKeys.empty)
    //        .withRelationshipPropertyKeys("TEST")()
    //
    //      val loadRef = model.findExactlyOne {
    //        case NoWhereBlock(s @ SourceBlock(_)) =>
    //          s.binds.fields shouldBe empty
    //      }
    //
    //      val nodeA = toField('a -> CTNode)
    //      val nodeB = toField('b -> CTNode)
    //      val rel = toField('r -> CTRelationship("TEST"))
    //
    //      val matchRef = model.findExactlyOne {
    //        case MatchBlock(deps, Pattern(fields, topo), exprs, _, _) =>
    //          fields should equal(Set(nodeA, nodeB))
    //          topo should equal(Map())
    //          exprs shouldBe empty
    //      }
    //
    //      val projectRef = model.findExactlyOne {
    //        case NoWhereBlock(ProjectBlock(deps, Fields(map, graphs), _, _, _)) =>
    //          map shouldBe empty
    //
    //          graphs shouldBe Set(
    //            IRPatternGraph(
    //              "moo",
    //              expectedSchema,
    //              Pattern(Set(nodeA, nodeB, rel), Map(rel -> DirectedRelationship(nodeA, nodeB)))))
    //      }
    //
    //      model.result match {
    //        case NoWhereBlock(ResultBlock(deps, items, _, _, _, _)) =>
    //          deps should equal(Set(projectRef))
    //          items.fields shouldBe empty
    //          items.graphs should equal(Set(IRCatalogGraph("moo", expectedSchema, QualifiedGraphName(SessionPropertyGraphDataSource.Namespace, GraphName("moo")))))
    //      }
    //
    //      model.requirements should equal(
    //        Map(
    //          projectRef -> Set(matchRef),
    //          matchRef -> Set(loadRef),
    //          loadRef -> Set()
    //        ))
    //    }
    //  }
  }

  describe("CreateGraphStatement") {
    it("can parse a CREATE GRAPH statement") {
      val innerQuery = s"FROM GRAPH ${testQualifiedGraphName.toString} RETURN GRAPH"

      val query =
        s"""
          |CREATE GRAPH session.bar {
          | $innerQuery
          |}
        """.stripMargin

      val result = query.parseIR[CreateGraphStatement[Expr]]

      result.innerQuery.model should equalWithTracing( innerQuery.asCypherQuery.model)
      result.graph.qualifiedGraphName should equal(QualifiedGraphName(Namespace("session"),GraphName("bar")))
      result.graph.schema should equal(testGraphSchema)
    }
  }

  describe("DeleteGraphStatement") {
    it("can parse a DELETE GRAPH statement") {
      val query = s"DELETE GRAPH $testQualifiedGraphName"

      val result = query.parseIR[DeleteGraphStatement[Expr]]

      result.graph.qualifiedGraphName should equal(testQualifiedGraphName)
      result.graph.schema should equal(testGraphSchema)
    }
  }

  implicit class RichBlock(b: Block[Expr]) {

    def findExactlyOne(f: PartialFunction[Block[Expr], Unit]): Block[Expr] = {
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

  implicit class RichModel(model: QueryModel[Expr]) {

    def ensureThat(f: (QueryModel[Expr], CypherMap) => Unit) = f(model, model.parameters)

  }

}
