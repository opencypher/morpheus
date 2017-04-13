package org.opencypher.spark.impl.syntax

import org.opencypher.spark.api.expr._

import scala.annotation.tailrec
import scala.language.implicitConversions

trait ExprSyntax {
  implicit def exprOps(e: Expr): ExprOps = new ExprOps(e)
}

final class ExprOps(val e: Expr) extends AnyVal {

  def dependencies: Set[Var] = computeDependencies(List(e), Set.empty)

  // TODO: Test this
  @tailrec
  private def computeDependencies(remaining: List[Expr], result: Set[Var]): Set[Var] = remaining match {
    case (expr: Var) :: tl => computeDependencies(tl, result + expr)
    case Equals(l, r) :: tl => computeDependencies(l :: r :: tl, result)
    case StartNode(expr) :: tl => computeDependencies(expr :: tl, result)
    case EndNode(expr) :: tl => computeDependencies(expr :: tl, result)
    case TypeId(expr) :: tl => computeDependencies(expr :: tl, result)
    case HasLabel(expr, _) :: tl => computeDependencies(expr :: tl, result)
    case HasType(expr, _) :: tl => computeDependencies(expr :: tl, result)
    case Property(expr, _) :: tl => computeDependencies(expr :: tl, result)
    case (expr: Ands) :: tl => computeDependencies(expr.exprs.toList ++ tl, result)
    case (expr: Ors) :: tl => computeDependencies(expr.exprs.toList ++ tl, result)
    case (expr: Lit[_]) :: tl => computeDependencies(tl, result)
    case (expr: Const) :: tl => computeDependencies(tl, result)
    case _ => result
  }
}
