package org.opencypher.spark.impl.parse

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, Rewriter, SemanticCheck, bottomUp}
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.StatementRewriter
import org.neo4j.cypher.internal.frontend.v3_3.phases.{BaseContext, Condition}

object sparkCypherRewriting extends StatementRewriter {
  override def instance(context: BaseContext): Rewriter = bottomUp(Rewriter.lift {
    case a@Ands(exprs) =>
      val (left, right) = exprs.partition {
        case _: HasLabels => true
        case _ => false
      }
      val singleRight: Expression = right.headOption match {
        case None => True()(a.position)
        case Some(expr) => right.tail.headOption match {
          case None => expr
          case _ => Ands(right)(a.position)
        }
      }
      RetypingPredicate(left, singleRight)(a.position)
  })

  override def description: String = "cos specific rewrites"

  override def postConditions: Set[Condition] = Set.empty
}

case class RetypingPredicate(left: Set[Expression], right: Expression)(val position: InputPosition) extends Expression {
  override def semanticCheck(ctx: Expression.SemanticContext): SemanticCheck =
    Ands(left + right)(position).semanticCheck(ctx)
}
