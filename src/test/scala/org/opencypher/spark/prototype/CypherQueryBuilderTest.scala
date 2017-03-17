package org.opencypher.spark.prototype

import org.opencypher.spark.prototype.api.expr.{Expr, HasLabel}
import org.opencypher.spark.prototype.api.ir.block._
import org.opencypher.spark.prototype.api.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.api.ir.pattern._
import org.opencypher.spark.prototype.api.ir.{CypherQuery, Field, QueryModel}

import scala.collection.immutable.Set

class CypherQueryBuilderTest extends IrTestSuite {

  test("match node and return it") {
    "MATCH (a:Person) RETURN a".model.ensureThat { (model, globals) =>
      import globals._

      val matchRef = model.findExactlyOne {
        case MatchBlock(_, Pattern(entities, topo), AllGiven(exprs)) =>
          entities should equal(Map(toField('a) -> EveryNode(AllOf(label("Person")))))
          topo shouldBe empty
          exprs should equal(Set(HasLabel(toVar('a), label("Person"))))
      }

      val projectRef = model.findExactlyOne {
        case NoWhereBlock(ProjectBlock(_, ProjectedFields(map), _)) =>
          map should equal(Map(toField('a) -> toVar('a)))
      }

      model.result match {
        case NoWhereBlock(ResultBlock(_, FieldsInOrder(Field("a")), _)) =>
      }

      model.requirements should equal(Map(
        projectRef -> Set(matchRef),
        matchRef -> Set()
      ))
    }
  }

  test("match simple relationship pattern and return some fields") {
    "MATCH (a)-[r]->(b) RETURN b AS otherB, a, r".model.ensureThat { (model, globals) =>

      val matchRef = model.findExactlyOne {
        case NoWhereBlock(MatchBlock(_, Pattern(entities, topo), _)) =>
          entities should equal(Map(toField('a) -> EveryNode, toField('b) -> EveryNode, toField('r) -> EveryRelationship))
          topo should equal(Map(toField('r) -> DirectedRelationship('a, 'b)))
      }

      val projectRef = model.findExactlyOne {
        case NoWhereBlock(ProjectBlock(_, ProjectedFields(map), _)) =>
          map should equal(Map(
            toField('a) -> toVar('a),
            toField('otherB) -> toVar('b),
            toField('r) -> toVar('r)
          ))
      }

      model.result match {
        case NoWhereBlock(ResultBlock(_, FieldsInOrder(Field("otherB"), Field("a"), Field("r")), _)) =>
      }

      model.requirements should equal(Map(
        projectRef -> Set(matchRef),
        matchRef -> Set()
      ))
    }
  }

  implicit class RichModel(model: QueryModel[Expr]) {

    def ensureThat(f: (QueryModel[Expr], GlobalsRegistry) => Unit) = f(model, model.globals)

    def requirements = {
      val deps = model.result.after
      val allDeps = deps.flatMap(model.allDependencies) ++ deps
      model.blocks.keySet should equal(allDeps)
      allDeps.map { ref => ref -> model.dependencies(ref) }.toMap
    }

    def findExactlyOne(f: PartialFunction[Block[Expr], Unit]): BlockRef = {
      val result = model.collect {
        case (ref, block) if f.isDefinedAt(block) => ref
      }
      withClue(s"Failed to extract single matching block from ${model.blocks}") {
        result.size should equal(1)
      }
      result.head
    }
  }
}
