package org.opencypher.spark.prototype.logical

import org.neo4j.cypher.internal.frontend.v3_2.helpers.fixedPoint
import org.opencypher.spark.prototype._
import org.opencypher.spark.prototype.ir._
import org.opencypher.spark.prototype.ir.block._
import org.opencypher.spark.prototype.ir.pattern.Pattern
import org.opencypher.spark.prototype.ir.token.TokenRegistry

import scala.collection.immutable.SortedSet

class LogicalOperatorProducer {

  def plan(ir: QueryDescriptor[Expr]): LogicalOperator = {
    val model = ir.model

    implicit val tokenDefs = model.tokens

    val plan = model(model.root) match {
      case MatchBlock(_, _, given, where, _) =>
        // plan given
        val plan = givenPlanner(given)
        // all variables are now projected to fields
        // and will be available to predicates
        val withFilters = wherePlanner(plan, where)

        withFilters
    }

    val finished = model.blocks.values.foldLeft(plan) {
      case (acc, next) => next match {
        case ProjectBlock(_, _, ProjectedFields(exprs), _) =>
          planProjections(acc, exprs.values.toSet)
        case SelectBlock(_, _, _, _) =>

          // all blocks planned, drop extra columns
          val map = SortedSet(ir.returns.map {
            case (s, f) =>
              val expr: Expr = Var(f.name)
              expr -> s
          }.toSeq: _*)(exprOrdering)
          Select(map, acc)
        case _ => acc
      }
    }

    finished
  }

  private def planProjections(in: LogicalOperator, exprs: Set[Expr])(implicit tokens: TokenRegistry) = {
    exprs.foldLeft(in) {
      case (acc, p: Property) =>
        Project(p, acc)
      case x => throw new UnsupportedOperationException(s"can not project $x")
    }
  }

  private def wherePlanner(in: LogicalOperator, where: Where[Expr])(implicit tokens: TokenRegistry) = {
    val equalities = where.predicates.foldLeft(in) {
      case (acc, eq@Equals(prop: Property, _: Param)) =>
        Filter(eq, Project(prop, acc))
      case (acc, _: HasLabel) => acc // ignore label predicates; solved by scans
      case (_, x) => throw new UnsupportedOperationException(s"Can't deal with $x")
    }

    equalities
  }

  private def givenPlanner(pattern: Pattern[Expr])(implicit tokens: TokenRegistry) = {
    val (lhsLeaf, solvedNode) = nodePlan(pattern)

    val (newPlan, solvedConns) = fixedPoint(planExpansions)(lhsLeaf -> solvedNode)

    if (solvedConns.solved)
      newPlan
    else
      throw new IllegalStateException("Given not solved!")
  }

  @scala.annotation.tailrec
  private def planExpansions(input: (LogicalOperator, Pattern[Expr])): (LogicalOperator, Pattern[Expr]) = {
    val (in, given) = input

    val knownVars = in.signature.items.flatMap(_.exprs.collect { case v: Var => v })

    val result: Option[ExpandOperator] = given.topology.collectFirst {
      case (r, c) =>
        knownVars.collectFirst {
          case v if Var(c.source.name) == v => ExpandSource(Var(c.source.name), Var(r.name), Var(c.target.name), in)
          case v if Var(c.target.name) == v => ExpandTarget(Var(c.source.name), Var(r.name), Var(c.target.name), in)
        }
    }.flatten

    result match {
      case None => input
      case Some(op) => planExpansions(op -> given.solvedConnection(Field(op.rel.name)))
    }
  }

  private def nodePlan(given: Pattern[Expr])(implicit tokens: TokenRegistry): (LogicalOperator, Pattern[Expr]) = {
    val (field, anyNode) = given.nodes.head
    NodeScan(Var(field.name), anyNode) -> given.solvedNode(field)
  }
}
