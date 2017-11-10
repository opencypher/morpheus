/*
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.typer

import java.lang.Integer.parseInt

import cats.data._
import cats.implicits._
import cats.{Foldable, Monoid}
import org.atnos.eff._
import org.atnos.eff.all._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast.functions.{Collect, Exists, Max, Min}
import org.opencypher.caps.api.schema.Schema
import org.opencypher.caps.api.types.CypherType.joinMonoid
import org.opencypher.caps.api.types._
import org.opencypher.caps.impl.parse.RetypingPredicate
import org.opencypher.caps.impl.spark.exception.Raise

import scala.util.Try

final case class SchemaTyper(schema: Schema) {

  def infer(expr: Expression, tracker: TypeTracker = TypeTracker.empty)
  : Either[NonEmptyList[TyperError], TyperResult[CypherType]] =
    SchemaTyper.processInContext[TyperStack[CypherType]](expr, tracker).run(schema)

  def inferOrThrow(expr: Expression, tracker: TypeTracker = TypeTracker.empty)
  : TyperResult[CypherType] =
    SchemaTyper.processInContext[TyperStack[CypherType]](expr, tracker).runOrThrow(schema)
}

object SchemaTyper {

  def processInContext[R: _hasSchema : _keepsErrors : _hasTracker : _logsTypes](expr: Expression, tracker: TypeTracker)
  : Eff[R, CypherType] = for {
    _ <- put[R, TypeTracker](tracker)
    result <- process[R](expr)
  } yield result

  def process[R: _hasSchema : _keepsErrors : _hasTracker : _logsTypes](expr: Expression): Eff[R, CypherType] = expr match {

    case _: Variable =>
      for {
        t <- typeOf[R](expr)
        _ <- recordType[R](expr -> t)
      } yield t

    case Parameter(name, _) =>
      for {
        t <- parameterType[R](name)
        _ <- recordType[R](expr -> t)
      } yield t

    case Property(v, PropertyKeyName(name)) =>
      // Map Void type to Null after joining, as properties can be null
      def voidToNull(tpe: CypherType): CypherType = if (tpe == CTVoid) {
        CTNull
      } else {
        tpe
      }

      for {
        varTyp <- process[R](v)
        schema <- ask[R, Schema]
        result <- varTyp.material match {
          case CTNode(labels) =>
            val propType = voidToNull {
              if (labels.isEmpty)
                schema.labels.map(schema.nodeKeys).foldLeft[CypherType](CTVoid) {
                  case (acc, next) => acc.join(next.getOrElse(name, CTVoid))
                }
              else {
                val keys = labels.map(schema.nodeKeys).reduce(_ ++ _)
                keys.getOrElse(name, CTVoid)
              }
            }
            recordType(v -> varTyp) >> recordAndUpdate(expr -> propType)

          case CTRelationship(types) =>
            val propType = voidToNull {
              if (types.isEmpty) {
                schema.relationshipTypes.map(schema.relationshipKeys).foldLeft[CypherType](CTVoid) {
                  case (acc, next) => acc.join(next.getOrElse(name, CTVoid))
                }
              } else {
                val keys = types.map(schema.relationshipKeys).reduceOption(_ ++ _)
                keys.getOrElse(Map.empty).getOrElse(name, CTVoid)
              }
            }
            recordType(v -> varTyp) >> recordAndUpdate(expr -> propType)

          case CTMap =>
            recordType(v -> varTyp) >> recordAndUpdate(expr -> CTWildcard)

          case _ =>
            error(InvalidContainerAccess(expr))
        }
      } yield result

    case HasLabels(node, labels) =>
      for {
        nodeType <- process[R](node)
        result <- nodeType.material match {
          case CTNode(nodeLabels) =>
            val detailed = nodeLabels ++ labels.map(_.name).toSet
            recordType[R](node -> nodeType) >>
              updateTyping[R](node -> CTNode(detailed)) >>
              recordAndUpdate[R](expr -> CTBoolean)

          case x =>
            error(InvalidType(node, CTNode, x))
        }
      } yield result

    case RetypingPredicate(left, rhs) =>
      for {
        result <- if (left.isEmpty) pure[R, CypherType](CTBoolean) else processAndsOrs(expr, left.toVector)
        rhsType <- process[R](rhs)
        _ <- recordType(rhs -> rhsType) >> recordAndUpdate(expr -> result)
      } yield result

    case Not(inner) =>
      for {
        innerType <- process[R](inner)
        result <- innerType.material match {
          case CTBoolean =>
            recordType(inner -> innerType) >>
              recordAndUpdate(expr -> CTBoolean)

          case x =>
            error(InvalidType(inner, CTBoolean, x))
        }
      } yield result

    case Ands(exprs) => processAndsOrs(expr, exprs.toVector)

    case Ors(exprs) => for {
      t1 <- get[R, TypeTracker]
      t2 <- put[R, TypeTracker](t1.pushScope()) >> get[R, TypeTracker]
      result <- processAndsOrs(expr, exprs.toVector)
      _ <- t2.popScope() match {
          case None => error(TypeTrackerScopeError) >> put[R, TypeTracker](TypeTracker.empty)
          case Some(t) =>
            put[R, TypeTracker](t)
        }
    } yield result

    case Equals(lhs, rhs) =>
      for {
        lhsType <- process[R](lhs)
        rhsType <- process[R](rhs)
        result <- recordTypes(lhs -> lhsType, rhs -> rhsType) >> recordAndUpdate(expr -> CTBoolean)
      } yield result

    case cmp: InequalityExpression =>
      for {
        lhsType <- process[R](cmp.lhs)
        rhsType <- process[R](cmp.rhs)
        result <- {
          val materialResultType = (lhsType.material, rhsType.material) match {
            case (CTInteger, CTFloat) => CTBoolean
            case (CTFloat, CTInteger) => CTBoolean
            case (x, y) if !x.couldBeSameTypeAs(y) => CTVoid
            case _ => CTBoolean
          }
          val resultType = materialResultType
            .asNullableAs(lhsType).asNullableAs(rhsType)
          recordTypes(cmp.lhs -> lhsType, cmp.rhs -> rhsType) >> recordAndUpdate(expr -> resultType)
        }
      } yield result

    case In(lhs, rhs) =>
      for {
        lhsType <- process[R](lhs)
        rhsType <- process[R](rhs)
        result <- rhsType match {
          case _: CTList => recordTypes(lhs -> lhsType, rhs -> rhsType) >> recordAndUpdate(expr -> CTBoolean)
          case x => error(InvalidType(rhs, CTList(CTWildcard), x))
        }
      } yield result

    case IsNull(inner) =>
      for {
        innerType <- process[R](inner)
        result <- recordTypes(inner -> innerType) >> recordAndUpdate(expr -> CTBoolean)
      } yield result

    case IsNotNull(inner) =>
      for {
        innerType <- process[R](inner)
        result <- recordTypes(inner -> innerType) >> recordAndUpdate(expr -> CTBoolean)
      } yield result

    case _: SignedDecimalIntegerLiteral =>
      recordAndUpdate(expr -> CTInteger)

    case _: DecimalDoubleLiteral =>
      recordAndUpdate(expr -> CTFloat)

    case _: BooleanLiteral =>
      recordAndUpdate(expr -> CTBoolean)

    case _: StringLiteral =>
      recordAndUpdate(expr -> CTString)

    case _: Null =>
      recordAndUpdate(expr -> CTNull)

    case ListLiteral(elts) =>
      for {
        eltType <- Foldable[Vector].foldMap(elts.toVector)(process[R])(joinMonoid)
        listTyp <- recordAndUpdate(expr -> CTList(eltType))
      } yield listTyp

    case expr: FunctionInvocation if expr.function == Exists =>
      expr.arguments match {
        case Seq(first: Property) =>
          for {
            _ <- process[R](first)
            existsType <- recordAndUpdate(expr -> CTBoolean)
          } yield existsType
        case Seq(nonProp) =>
          error(InvalidArgument(expr, nonProp))
        case seq =>
          error(WrongNumberOfArguments(expr, 1, seq.size))
      }

    case expr: FunctionInvocation if expr.function == Collect =>
      for {
        argExprs <- pure(expr.arguments)
        argTypes <- EffMonad.traverse(argExprs.toList)(process[R])
        computedType <- argTypes.reduceLeftOption(_ join _) match {
          case Some(innerType) =>
            pure[R, CypherType](CTList(innerType.nullable).nullable)
          case None =>
            error(NoSuitableSignatureForExpr(expr, argTypes))
        }
        result <- recordAndUpdate(expr -> computedType)
      } yield result

    case expr: FunctionInvocation =>
      FunctionInvocationTyper(expr)

    case CountStar() =>
      recordAndUpdate(expr -> CTInteger)

    case add: Add =>
      AddTyper(add)

    case sub: Subtract =>
      processArithmeticExpressions(sub)

    case mul: Multiply =>
      processArithmeticExpressions(mul)

    case div: Divide =>
      processArithmeticExpressions(div)

    case indexing@ContainerIndex(list@ListLiteral(exprs), index: SignedDecimalIntegerLiteral)
      if Try(parseInt(index.stringVal)).nonEmpty =>
      for {
        listType <- process[R](list)
        indexType <- process[R](index)
        eltType <- {
          Try(parseInt(index.stringVal)).map { rawPos =>
            val pos = if (rawPos < 0) exprs.size + rawPos else rawPos
            typeOf[R](exprs(pos))
          }.getOrElse(pure[R, CypherType](CTVoid))
        }
        result <- updateTyping(indexing -> eltType)
      } yield result

    case indexing@ContainerIndex(list, index) =>
      for {
        listTyp <- process[R](list)
        indexTyp <- process[R](index)
        result <- (listTyp, indexTyp.material) match {

          // TODO: Test all cases
          case (CTList(eltTyp), CTInteger) =>
            updateTyping(expr -> eltTyp.asNullableAs(indexTyp join eltTyp))

          case (CTListOrNull(eltTyp), CTInteger) =>
            updateTyping(expr -> eltTyp.nullable)

          case (typ, CTString) if typ.subTypeOf(CTMap).maybeTrue =>
            updateTyping(expr -> CTWildcard.nullable)

          case _ =>
            error(InvalidContainerAccess(indexing))
        }
      } yield result

    case _ =>
      error(UnsupportedExpr(expr))
  }

  private def processArithmeticExpressions[R: _hasSchema : _keepsErrors : _hasTracker : _logsTypes]
  (expr: Expression with BinaryOperatorExpression)
  : Eff[R, CypherType] = {
    for {
      lhsType <- process[R](expr.lhs)
      rhsType <- process[R](expr.rhs)
      result <- {
        numericTypeOrError(expr, lhsType, rhsType) match {
          case Right(t) =>
            val typ = if (lhsType.isNullable || rhsType.isNullable) t.nullable else t
            recordTypes(expr.lhs -> lhsType, expr.rhs -> rhsType) >> recordAndUpdate(expr -> typ)
          case Left((e, t)) =>
            recordTypes(expr.lhs -> lhsType, expr.rhs -> rhsType) >> error(InvalidType(e, Seq(CTInteger, CTFloat, CTNumber), t))
        }
      }
    } yield result
  }

  private def numericTypeOrError(expr: Expression with BinaryOperatorExpression, lhsType: CypherType, rhsType: CypherType) = (lhsType.material, rhsType.material) match {
    case (CTInteger, CTInteger) => Right(CTInteger)
    case (CTFloat, CTInteger) => Right(CTFloat)
    case (CTInteger, CTFloat) => Right(CTFloat)
    case (CTFloat, CTFloat) => Right(CTFloat)
    case (CTNumber, y) if y.subTypeOf(CTNumber).isTrue => Right(CTNumber)
    case (x, CTNumber) if x.subTypeOf(CTNumber).isTrue => Right(CTNumber)
    case (x, _) if !x.couldBeSameTypeAs(CTNumber) => Left(expr.lhs -> lhsType)
    case (_, y) if !y.couldBeSameTypeAs(CTNumber) => Left(expr.rhs -> rhsType)
    case _ => Right(CTAny)
  }

  private def processAndsOrs[R : _hasSchema : _keepsErrors : _hasTracker : _logsTypes](expr: Expression, orderedExprs: Vector[Expression]): Eff[R, CypherType] = {
    for {
      innerTypes <- EffMonad[R].sequence(orderedExprs.map(process[R]))
      result <- {
        val typeErrors = innerTypes.zipWithIndex.collect {
          case (t, idx) if t != CTBoolean =>
            error(InvalidType(orderedExprs(idx), CTBoolean, t))
        }
        if (typeErrors.isEmpty) {
          recordTypes(orderedExprs.zip(innerTypes): _*) >>
            recordAndUpdate(expr -> CTBoolean)
        }
        else typeErrors.reduce(_ >> _)
      }
    } yield result
  }

  private implicit class LiftedMonoid[R, A](val monoid: Monoid[A]) extends AnyVal with Monoid[Eff[R, A]] {
    override def empty: Eff[R, A] =
      pure(monoid.empty)

    override def combine(x: Eff[R, A], y: Eff[R, A]): Eff[R, A] =
      for {xTyp <- x; yTyp <- y} yield monoid.combine(xTyp, yTyp)
  }

  private sealed trait ExpressionTyper[T <: Expression] {
    def apply[R : _hasSchema : _keepsErrors : _hasTracker : _logsTypes](expr: T): Eff[R, CypherType]
  }

  private sealed trait SignatureBasedInvocationTyper[T <: Expression] extends ExpressionTyper[T] {

    def apply[R : _hasSchema : _keepsErrors : _hasTracker : _logsTypes](expr: T): Eff[R, CypherType] =
      for {
        argExprs <- pure(expr.arguments)
        argTypes <- EffMonad.traverse(argExprs.toList)(process[R])
        arguments <- pure(argExprs.zip(argTypes))
        signatures <- selectSignaturesFor(expr, arguments)
        computedType <- signatures.map(_._2).reduceLeftOption(_ join _) match {
          case Some(outputType) =>
            pure[R, CypherType](if (argTypes.exists(_.isNullable)) outputType.nullable else outputType)

          case None =>
            error(NoSuitableSignatureForExpr(expr, argTypes))
        }
        resultType <- recordAndUpdate(expr -> computedType)
      }
      yield resultType

    protected def selectSignaturesFor[R : _hasSchema : _keepsErrors : _hasTracker : _logsTypes]
      (expr: T, args: Seq[(Expression, CypherType)])
    : Eff[R, Set[(Seq[CypherType], CypherType)]] =
      for {
        signatures <- generateSignaturesFor(expr, args)
        eligible <- pure(signatures.flatMap { sig =>
          val (sigInputTypes, sigOutputType) = sig

          val compatibleArity = sigInputTypes.size == args.size
          val sigArgTypes = sigInputTypes.zip(args.map(_._2))
          val compatibleTypes = sigArgTypes.forall(((_: CypherType) couldBeSameTypeAs (_: CypherType)).tupled)

          if (compatibleArity && compatibleTypes) Some(sigInputTypes -> sigOutputType) else None
        })
      }
      yield eligible

    protected def generateSignaturesFor[R : _hasSchema : _keepsErrors : _hasTracker : _logsTypes]
      (expr: T, args: Seq[(Expression, CypherType)])
    : Eff[R, Set[(Seq[CypherType], CypherType)]]
  }

  private case object FunctionInvocationTyper extends SignatureBasedInvocationTyper[FunctionInvocation] {
    override protected def generateSignaturesFor[R : _hasSchema : _keepsErrors : _hasTracker : _logsTypes]
      (expr: FunctionInvocation, args: Seq[(Expression, CypherType)])
    : Eff[R, Set[(Seq[CypherType], CypherType)]] =
      expr.function match {
        case f: ExpressionCallTypeChecking =>
          pure(f.signatures.map { sig =>
            val sigInputTypes = sig.argumentTypes.map(fromFrontendType)
            val sigOutputType = fromFrontendType(sig.outputType)
            sigInputTypes -> sigOutputType
          }.toSet)

        case Min | Max =>
          pure[R, Set[(Seq[CypherType], CypherType)]](Set(
            Seq(CTInteger) -> CTInteger,
            Seq(CTFloat) -> CTFloat
          ))

        case _ =>
          wrong[R, TyperError](UnsupportedExpr(expr)) >> pure(Set.empty)
      }
  }

  private case object AddTyper extends SignatureBasedInvocationTyper[Add] {
    override protected def generateSignaturesFor[R : _hasSchema : _keepsErrors : _hasTracker : _logsTypes]
    (expr: Add, args: Seq[(Expression, CypherType)])
    : Eff[R, Set[(Seq[CypherType], CypherType)]] = {
      val (_, left) = args.head
      val (_, right) = args(1)
      (left.material -> right.material match {
        case (left: CTList, _) => Some(left listConcatJoin right)
        case (_, right: CTList) => Some(right listConcatJoin left)
        case (CTString, _) if right.subTypeOf(CTNumber).maybeTrue => Some(CTString)
        case (_, CTString) if left.subTypeOf(CTNumber).maybeTrue => Some(CTString)
        case (CTString, CTString) => Some(CTString)
        case (l, r) => numericTypeOrError(expr, l, r) match {
          case Right(t) => Some(t)
          case _ => None
        }
      }) match {
        case Some(t) => pure(Set(Seq(left, right) -> t.asNullableAs(left join right)))
        case None => pure(Set.empty)
      }
    }

      private implicit class RichCTList(val left: CTList) extends AnyVal {
        def listConcatJoin(right: CypherType): CypherType = (left, right) match {
          case (CTList(lInner), CTList(rInner)) => CTList(lInner join rInner)
          case (CTList(CTString), CTString) => left
          case (CTList(CTInteger), CTInteger) => left
          case (CTList(CTFloat), CTInteger) => CTList(CTNumber)
          case (CTList(CTVoid), _) => CTList(right)
          case _ => Raise.typeInferenceFailed(s"Could not infer common type of $left and $right")
        }
      }
  }
}


