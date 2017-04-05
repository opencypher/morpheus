package org.opencypher.spark.impl

import cats.Show
import cats.data._
import cats.syntax.all._
import org.atnos.eff._
import org.atnos.eff.all._
import org.atnos.eff.syntax.all._
import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types._

package object typer {

  type _keepsErrors[R] = KeepsErrors |= R
  type _hasContext[R] = HasContext |= R
  type _hasSchema[R] = HasSchema |= R

  type KeepsErrors[A] = Validate[TyperError, A]
  type HasContext[A] = State[TyperContext, A]
  type HasSchema[A] = Reader[Schema, A]

  type TyperStack[A] = Fx.fx3[HasSchema, KeepsErrors, HasContext]

  implicit final class RichTyperStack[A](val program: Eff[TyperStack[A], A]) extends AnyVal {

    def runOrThrow(schema: Schema): TyperResult[A] =
      run(schema) match {
        case Left(failures) =>
          throw new IllegalArgumentException(
            s"Errors during schema-based expression typing: ${failures.toList.mkString(", ")}"
          )

        case Right(result) =>
          result
      }

    def run(schema: Schema): Either[NonEmptyList[TyperError], TyperResult[A]] = {
      val rawResult: (Either[NonEmptyList[TyperError], A], TyperContext) = program
        .runReader(schema)
        .runNel[TyperError]
        .runState(TyperContext.empty)
        .run

      rawResult match {
        case (Left(errors), _) => Left(errors)
        case (Right(value), context) => Right(TyperResult(value, context))
      }
    }
  }

  // These combinators just delegate to the current context

  def typeOf[R : _keepsErrors : _hasContext](it: Expression): Eff[R, CypherType] =
    get[R, TyperContext] >>= { _.getTypeOf(it) }

  def updateTyping[R : _hasContext : _keepsErrors](entry: (Expression, CypherType)): Eff[R, CypherType] =
    get[R, TyperContext] >>= { _.putUpdated(entry) }

  def error[R : _keepsErrors](failure: TyperError): Eff[R, CypherType] =
    wrong[R, TyperError](failure) >> pure(CTWildcard)

  implicit val showExpr = new Show[Expression] {
    override def show(it: Expression): String = s"$it [${it.position}]"
  }
}
