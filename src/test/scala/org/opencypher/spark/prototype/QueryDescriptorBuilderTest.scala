package org.opencypher.spark.prototype

import org.opencypher.spark.prototype.ir.block._
import org.opencypher.spark.prototype.ir.global.GlobalsRegistry
import org.opencypher.spark.prototype.ir.pattern.{AnyNode, AnyRelationship, DirectedRelationship, Pattern}
import org.opencypher.spark.prototype.ir.{QueryDescriptor, QueryModel}

import scala.collection.immutable.Set

class QueryDescriptorBuilderTest extends IrTestSupport {

  test("simple match and return") {
    val q = "MATCH (a) RETURN a"

    val model = modelOf(q)

    val projectRef = model.find {
      case (ref, NoWhere(ProjectBlock(_, _, ProjectedFields(map), _))) =>
        map should equal(Map(toField('a) -> toVar('a)))
        ref
    }.get

    val matchRef = model.find {
      case (ref, NoWhere(MatchBlock(_, _, Pattern(entities, topo), _, _))) =>
        entities should equal(Map(toField('a) -> AnyNode()))
        topo shouldBe empty
        ref
    }.get

    val resultRef = model.find {
      case (ref, NoWhere(ResultBlock(_, _, ResultFields(fields), _))) =>
        fields should equal(Seq(toField('a)))
        ref
    }.get

    model(resultRef).after should equal(Set(projectRef))
    model(projectRef).after should equal(Set(matchRef))
    model(matchRef).after shouldBe empty
    model.blocks.keySet should equal(Set(projectRef, matchRef, resultRef))
  }

  test("match with pattern") {
    val q = "MATCH (a)-[r]->(b) RETURN b AS otherB, a, r"

    val model = modelOf(q)

    val matchRef = model.find {
      case (ref, NoWhere(MatchBlock(_, _, Pattern(entities, topo), _, _))) =>
        entities should equal(Map(toField('a) -> AnyNode(), toField('b) -> AnyNode(), toField('r) -> AnyRelationship()))
        topo should equal(Map(toField('r) -> DirectedRelationship('a, 'b)))
        ref
    }.get

    val projectRef = model.find {
      case (ref, NoWhere(ProjectBlock(_, _, ProjectedFields(map), _))) =>
        map should equal(Map(
          toField('a) -> toVar('a),
          toField('otherB) -> toVar('b),
          toField('r) -> toVar('r)
        ))
        ref
    }.get

    val resultRef = model.find {
      case (ref, NoWhere(ResultBlock(_, _, ResultFields(fields), _))) =>
        fields should equal(Seq(toField('otherB), toField('a), toField('r)))
        ref
    }.get

    model(resultRef).after should equal(Set(projectRef))
    model(projectRef).after should equal(Set(matchRef))
    model(matchRef).after shouldBe empty
    model.blocks.keySet should equal(Set(projectRef, matchRef, resultRef))
  }

  private def modelOf(q: String): QueryModel[Expr] = irFor(q).model

  private def irFor(q: String): QueryDescriptor[Expr] = {
    val (stmt, _) = CypherParser.parseAndExtract(q)
    QueryDescriptorBuilder.from(stmt, q, GlobalsExtractor(stmt))
  }

  implicit class RichModel(model: QueryModel[Expr]) {
    def ensureThat(f: (QueryModel[Expr], GlobalsRegistry) => Unit) = f(model, model.globals)
  }

  object NoWhere {
    def unapply[E](block: Block[E]): Option[Block[E]] =
      if (block.where.predicates.isEmpty) Some(block) else None
  }
}
