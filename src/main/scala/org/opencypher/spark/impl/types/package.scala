package org.opencypher.spark.impl

import cats.data._
import cats.syntax.all._
import org.atnos.eff._
import org.atnos.eff.all._
import org.atnos.eff.syntax.all._
import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.schema.Schema
import org.opencypher.spark.api.types.CTWildcard

package object types {

  type _mayFail[R] = MayFail |= R
  type _hasContext[R] = HasContext |= R
  type _hasSchema[R] = HasSchema |= R

  type MayFail[A] = Validate[TyperError, A]
  type HasContext[A] = State[TyperContext, A]
  type HasSchema[A] = Reader[Schema, A]

  type TyperStack[A] = Fx.fx3[HasSchema, MayFail, HasContext]

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

  def typeOf[R : _mayFail : _hasContext](it: Expression): Eff[R, CypherType] =
    get[R, TyperContext] >>= { _.getTypeOf(it) }

  def updateTyping[R : _hasContext : _mayFail](entry: (Expression, CypherType)): Eff[R, CypherType] =
    get[R, TyperContext] >>= { _.putUpdated(entry) }

  def error[R : _mayFail](failure: TyperError): Eff[R, CypherType] =
    wrong[R, TyperError](failure) >> pure(CTWildcard)
}
