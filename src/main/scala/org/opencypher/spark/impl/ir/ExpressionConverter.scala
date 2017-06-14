package org.opencypher.spark.impl.ir

import org.neo4j.cypher.internal.frontend.v3_2.ast.functions
import org.neo4j.cypher.internal.frontend.v3_2.{Ref, ast}
import org.opencypher.spark.api.expr._
import org.opencypher.spark.api.ir.global.GlobalsRegistry
import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.parse.RetypingPredicate

import scala.language.implicitConversions

final class ExpressionConverter(val globals: GlobalsRegistry) extends AnyVal {

  implicit def toRef(e: ast.Expression): Ref[ast.Expression] = Ref(e)

  def convert(e: ast.Expression)(implicit typings: (Ref[ast.Expression]) => CypherType): Expr = e match {
    case ast.Variable(name) => Var(name)(typings(e))
    case ast.Parameter(name, _) => Const(globals.constantRefByName(name))(typings(e))

      // Literals
    case astExpr: ast.IntegerLiteral => IntegerLit(astExpr.value)(typings(e))
    case ast.StringLiteral(value) => StringLit(value)(typings(e))
    case _: ast.True => TrueLit()
    case _: ast.False => FalseLit()

    case ast.Property(m, ast.PropertyKeyName(name)) => Property(convert(m), globals.propertyKeyRefByName(name))(typings(e))

      // Predicates
    case ast.Ands(exprs) => new Ands(exprs.map(convert))(typings(e))
    case ast.HasLabels(node, labels) =>
      val exprs = labels.map { (l: ast.LabelName) => HasLabel(convert(node), globals.labelByName(l.name))(typings(e)) }
      if (exprs.size == 1) exprs.head else new Ands(exprs.toSet)(typings(e))
    case ast.Not(expr) => Not(convert(expr))(typings(e))
    // MATCH ()-[r]->() WHERE type(r) IN ['FOO', 'BAR] ==> MATCH ()-[r]->() WHERE r[:FOO|BAR]
    case ast.Equals(f: ast.FunctionInvocation, s: ast.StringLiteral) if f.function == functions.Type =>
      HasType(convert(f.args.head), globals.relTypeByName(s.value))(CTBoolean)
    case ast.Equals(lhs, rhs) => Equals(convert(lhs), convert(rhs))(typings(e))
    case ast.LessThan(lhs, rhs) => LessThan(convert(lhs), convert(rhs))(typings(e))
    case ast.In(lhs, ast.ListLiteral(elems)) if elems.size == 1 =>
      Equals(convert(lhs), convert(elems.head))(typings(e))
    case RetypingPredicate(lhs, rhs) =>
      new Ands(lhs.map(convert) + convert(rhs))(typings(e))

    // Arithmetics
    case ast.Subtract(lhs, rhs) => Subtract(convert(lhs), convert(rhs))(typings(e))

    case _ => throw new NotImplementedError(s"Not yet able to convert expression: $e")
  }
}
