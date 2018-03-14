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
 */
package org.opencypher.okapi.ir.impl

import org.opencypher.okapi.api.schema.{PropertyKeys, Schema}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.block._
import org.opencypher.okapi.ir.api.expr.{Expr, HasLabel, Property, Var}
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.test._

import scala.collection.immutable.Set

class IrBuilderTest extends IrTestSuite {

  it("computes a pattern graph schema correctly - 1 create") {
    val query =
      """
        |CONSTRUCT  {
        |  CREATE (a :A)
        |}
        |RETURN GRAPH""".stripMargin

    query.model.ensureThat { (model, _) =>
      model.result match {
        case GraphResultBlock(_, IRPatternGraph(schema, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A")())
        case _ => fail("no matching graph result found")
      }
    }
  }

  it("computes a pattern graph schema correctly - 2 creates") {
    val query =
      """
        |CONSTRUCT  {
        |  CREATE (a :A)
        |  CREATE (b :B:C)
        |}
        |RETURN GRAPH""".stripMargin

    query.model.ensureThat { (model, _) =>
      model.result match {
        case GraphResultBlock(_, IRPatternGraph(schema, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A")().withNodePropertyKeys("B", "C")())
        case _ => fail("no matching graph result found")
      }
    }
  }

  it("computes a pattern graph schema correctly - 2 creates and a set") {
    val query =
      """
        |CONSTRUCT  {
        |  CREATE (a :A)
        |  CREATE (b :B:C)
        |  SET a :D
        |}
        |RETURN GRAPH""".stripMargin

    query.model.ensureThat { (model, _) =>
      model.result match {
        case GraphResultBlock(_, IRPatternGraph(schema, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "D")().withNodePropertyKeys("B", "C")())
        case _ => fail("no matching graph result found")
      }
    }
  }

  it("computes a pattern graph schema correctly - 1 create and 2 sets") {
    val query =
      """
        |CONSTRUCT  {
        |  CREATE (a :A)
        |  SET a :B
        |  SET a :C
        |}
        |RETURN GRAPH""".stripMargin

    query.model.ensureThat { (model, _) =>
      model.result match {
        case GraphResultBlock(_, IRPatternGraph(schema, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "B", "C")())
        case _ => fail("no matching graph result found")
      }
    }
  }

  it("computes a pattern graph schema correctly - 2 creates and 2 sets on same label") {
    val query =
      """
        |CONSTRUCT  {
        |  CREATE (a :A)
        |  CREATE (b :A)
        |  SET a :B
        |  SET b :C
        |}
        |RETURN GRAPH""".stripMargin

    query.model.ensureThat { (model, _) =>
      model.result match {
        case GraphResultBlock(_, IRPatternGraph(schema, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "B")().withNodePropertyKeys("A", "C")())
        case _ => fail("no matching graph result found")
      }
    }
  }

  it("computes a pattern graph schema correctly - 2 creates and 2 sets on different labels") {
    val query =
      """
        |CONSTRUCT  {
        |  CREATE (a :A)
        |  CREATE (b :B)
        |  SET a :B
        |  SET b :A
        |}
        |RETURN GRAPH""".stripMargin

    query.model.ensureThat { (model, _) =>
      model.result match {
        case GraphResultBlock(_, IRPatternGraph(schema, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "B")())
        case _ => fail("no matching graph result found")
      }
    }
  }

  it("computes a pattern graph schema correctly - 1 create and 1 set property") {
    val query =
      """
        |CONSTRUCT  {
        |  CREATE (a :A)
        |  SET a.name = 'Mats'
        |}
        |RETURN GRAPH""".stripMargin

    query.model.ensureThat { (model, _) =>
      model.result match {
        case GraphResultBlock(_, IRPatternGraph(schema, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A")("name" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }
  }

  it("computes a pattern graph schema correctly - 1 create and 1 set property with two labels") {
    val query =
      """
        |CONSTRUCT  {
        |  CREATE (a :A:B)
        |  SET a.name = 'Mats'
        |}
        |RETURN GRAPH""".stripMargin

    query.model.ensureThat { (model, _) =>
      model.result match {
        case GraphResultBlock(_, IRPatternGraph(schema, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "B")("name" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }
  }

  it("computes a pattern graph schema correctly - 1 create and 1 set rel property") {
    val query =
      """
        |CONSTRUCT  {
        |  CREATE ()-[r :R]->()
        |  SET r.level = 'high'
        |}
        |RETURN GRAPH""".stripMargin

    query.model.ensureThat { (model, _) =>
      model.result match {
        case GraphResultBlock(_, IRPatternGraph(schema, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys(Set.empty[String]).withRelationshipPropertyKeys("R")("level" -> CTString))
        case _ => fail("no matching graph result found")
      }
    }
  }

  it("computes a pattern graph schema correctly - 1 create and 2 set properties") {
    val query =
      """
        |CONSTRUCT  {
        |  CREATE (a :A)
        |  SET a.category = 'computer'
        |  SET a.ports = 4
        |}
        |RETURN GRAPH""".stripMargin

    query.model.ensureThat { (model, _) =>
      model.result match {
        case GraphResultBlock(_, IRPatternGraph(schema, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys(Set("A"), PropertyKeys("category" -> CTString, "ports" -> CTInteger)))
        case _ => fail("no matching graph result found")
      }
    }
  }

  it("computes a pattern graph schema correctly - 1 create from equivalent") {
    val query =
      """
        |CONSTRUCT {
        |  CREATE (a :A)
        |}
        |CONSTRUCT {
        |  CREATE (c~a)
        |  SET c :B
        |}
        |RETURN GRAPH""".stripMargin

    query.model.ensureThat { (model, _) =>
      model.result match {
        case GraphResultBlock(_, IRPatternGraph(schema, _, _)) =>
          schema should equal(Schema.empty.withNodePropertyKeys("A", "B")())
        case _ => fail("no matching graph result found")
      }
    }
  }

  test("match node and return it") {
    "MATCH (a:Person) RETURN a".model.ensureThat { (model, globals) =>
      val loadRef = model.findExactlyOne {
        case NoWhereBlock(s@SourceBlock(_)) =>
          s.binds.fields shouldBe empty
      }

      val matchRef = model.findExactlyOne {
        case MatchBlock(deps, Pattern(fields, topo, equivalences), exprs, _, _) =>
          deps should equal(Set(loadRef))
          fields should equal(Set(toField('a -> CTNode)))
          topo shouldBe empty
          exprs should equal(Set(HasLabel(toVar('a), Label("Person"))()))
      }

      val projectRef = model.findExactlyOne {
        // TODO: Properly assert on graphs, also below
        case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) =>
          deps should equal(Set(matchRef))
          map should equal(Map(toField('a) -> toVar('a)))
      }

      model.result match {
        case NoWhereBlock(TableResultBlock(deps, OrderedFields(IndexedSeq(IRField("a"))), _, _, _)) =>
          deps should equal(Set(projectRef))
      }

      model.requirements should equal(
        Map(
          projectRef -> Set(matchRef),
          matchRef -> Set(loadRef),
          loadRef -> Set()
        ))
    }
  }

  test("match simple relationship pattern and return some fields") {
    "MATCH (a)-[r]->(b) RETURN b AS otherB, a, r".model.ensureThat { (model, globals) =>
      val loadRef = model.findExactlyOne {
        case NoWhereBlock(s@SourceBlock(_)) =>
          s.binds.fields shouldBe empty
      }

      val matchRef = model.findExactlyOne {
        case NoWhereBlock(MatchBlock(deps, Pattern(fields, topo, equivalences), _, _, _)) =>
          deps should equal(Set(loadRef))
          fields should equal(Set[IRField]('a -> CTNode, 'b -> CTNode, 'r -> CTRelationship))
          val map = Map(toField('r) -> DirectedRelationship('a, 'b))
          topo should equal(map)
      }

      val projectRef = model.findExactlyOne {
        // TODO: Properly assert on graphs, also below
        case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) =>
          deps should equal(Set(matchRef))
          map should equal(
            Map(
              toField('a) -> toVar('a),
              toField('otherB) -> toVar('b),
              toField('r) -> toVar('r)
            ))
      }

      model.result match {
        case NoWhereBlock(TableResultBlock(_, OrderedFields(IndexedSeq(IRField("otherB"), IRField("a"), IRField("r"))), _, _, _)) =>
      }

      model.requirements should equal(
        Map(
          projectRef -> Set(matchRef),
          matchRef -> Set(loadRef),
          loadRef -> Set()
        ))
    }
  }

  test("match node order by name and return it") {
    "MATCH (a:Person) WITH a.name AS name, a.age AS age ORDER BY age RETURN age, name".model.ensureThat {
      (model, globals) =>
        val loadRef = model.findExactlyOne {
          case NoWhereBlock(s@SourceBlock(_)) =>
            s.binds.fields shouldBe empty
        }

        val matchRef = model.findExactlyOne {
          case MatchBlock(deps, Pattern(fields, topo, equivalences), exprs, _, _) =>
            deps should equal(Set(loadRef))
            fields should equal(Set(toField('a -> CTNode)))
            topo shouldBe empty
            exprs should equal(Set(HasLabel(toVar('a), Label("Person"))()))
        }

        val projectRef = model.findExactlyOne {
          case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) if deps.head == matchRef =>
            deps should equal(Set(matchRef))
            map should equal(
              Map(
                toField('name) -> Property(Var("a")(CTNode), PropertyKey("name"))(CTNull),
                toField('age) -> Property(Var("a")(CTNode), PropertyKey("age"))(CTNull)
              ))
        }

        val project2Ref = model.findExactlyOne {
          case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) if deps.head == projectRef =>
            deps should equal(Set(projectRef))
            map should equal(
              Map(
                toField('age) -> toVar('age),
                toField('name) -> toVar('name)
              ))
        }

        val orderByRef = model.findExactlyOne {
          case NoWhereBlock(OrderAndSliceBlock(deps, orderBy, None, None, _)) =>
            val ordered = Vector(Asc(toVar('age)))
            orderBy should equal(ordered)
            deps should equal(Set(project2Ref))
        }

        val project3Ref = model.findExactlyOne {
          case NoWhereBlock(ProjectBlock(deps, Fields(map), _, _, _)) if deps.head == orderByRef =>
            deps should equal(Set(orderByRef))
            map should equal(
              Map(
                toField('age) -> toVar('age),
                toField('name) -> toVar('name)
              ))
        }

        model.result match {
          case NoWhereBlock(TableResultBlock(deps, OrderedFields(IndexedSeq(IRField("age"), IRField("name"))), _, _, _)) =>
            deps should equal(Set(project3Ref))
        }

        model.requirements should equal(
          Map(
            project3Ref -> Set(orderByRef),
            orderByRef -> Set(project2Ref),
            project2Ref -> Set(projectRef),
            projectRef -> Set(matchRef),
            matchRef -> Set(loadRef),
            loadRef -> Set()
          ))
    }
  }

  //  ignore("can handle return graph of") {
  //    "MATCH (a), (b) RETURN GRAPH moo OF (a)-[r:TEST]->(b)".model.ensureThat { (model, globals) =>
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
  //        // TODO: Properly assert on graphs, also below
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

  implicit class RichModel(model: QueryModel[Expr]) {

    def ensureThat(f: (QueryModel[Expr], CypherMap) => Unit) = f(model, model.parameters)

    def requirements = {
      val deps = model.result.after
      val allDeps = deps.flatMap(model.allDependencies) ++ deps
      model.blocks.keySet should equal(allDeps)
      allDeps.map { ref =>
        ref -> model.dependencies(ref)
      }.toMap
    }

    def findExactlyOne(f: PartialFunction[Block[Expr], Unit]): BlockRef = {
      val result = model.collect {
        case (ref, block) if f.isDefinedAt(block) =>
          f(block)
          ref
      }
      withClue(s"Failed to extract single matching block from ${model.blocks}") {
        result.size should equal(1)
      }
      result.head
    }
  }

}
