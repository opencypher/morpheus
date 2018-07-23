/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.ir.impl.typer

import java.lang.Integer.parseInt

import cats.data._
import cats.implicits._
import cats.{Foldable, Monoid}
import org.atnos.eff._
import org.atnos.eff.all._
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType.joinMonoid
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.ir.impl.parse.rewriter.ExistsPattern
import org.opencypher.v9_1.expressions._
import org.opencypher.v9_1.expressions.functions.{Coalesce, Collect, Exists, Max, Min, ToString, ToBoolean}

import scala.util.Try

final case class SchemaTyper(schema: Schema) {

  def infer(
      expr: Expression,
      tracker: TypeTracker = TypeTracker.empty): Either[NonEmptyList[TyperError], TyperResult[CypherType]] =
    SchemaTyper.processInContext[TyperStack[CypherType]](expr, tracker).run(schema)

  def inferOrThrow(expr: Expression, tracker: TypeTracker = TypeTracker.empty): TyperResult[CypherType] =
    SchemaTyper.processInContext[TyperStack[CypherType]](expr, tracker).runOrThrow(schema)
}

object SchemaTyper {

  def processInContext[R: _hasSchema: _keepsErrors: _hasTracker: _logsTypes](
      expr: Expression,
      tracker: TypeTracker): Eff[R, CypherType] =
    for {
      _ <- put[R, TypeTracker](tracker)
      result <- process[R](expr)
    } yield result

  def process[R: _hasSchema: _keepsErrors: _hasTracker: _logsTypes](expr: Expression): Eff[R, CypherType] = expr match {

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
      for {
        varTyp <- process[R](v)
        schema <- ask[R, Schema]
        result <- varTyp.material match {

          // This means that the node can have any possible label combination, as the user did not specify any constraints
          case n: CTNode if n.labels.isEmpty =>
            val propType = schema.allLabelCombinations
              .map(l => schema.nodeKeyType(l, name).getOrElse(CTNull))
              .foldLeft(CTVoid: CypherType)(_ join _)
            recordType(v -> varTyp) >> recordAndUpdate(expr -> propType)

          // User specified label constraints - we can use those for type inference
          case CTNode(labels, _) =>
            val propType = schema.nodeKeyType(labels, name).getOrElse(CTNull)
            recordType(v -> varTyp) >> recordAndUpdate(expr -> propType)

          case CTRelationship(types, _) =>
            val propType = schema.relationshipKeyType(types, name).getOrElse(CTNull)
            recordType(v -> varTyp) >> recordAndUpdate(expr -> propType)

          case CTMap =>
            recordType(v -> varTyp) >> recordAndUpdate(expr -> CTWildcard)

          case _ =>
            error(InvalidContainerAccess(expr))
        }
      } yield result

    case MapExpression(items) =>
      val values = items.map(_._2)
      for {
        _ <- values.toList.traverse(process[R])
        mapType <- recordAndUpdate(expr -> CTMap)
      } yield mapType

    case _: ExistsPattern =>
      recordAndUpdate[R](expr -> CTBoolean)

    case HasLabels(node, labels) =>
      for {
        nodeType <- process[R](node)
        result <- nodeType.material match {
          case CTNode(nodeLabels, qgn) =>
            val detailed = nodeLabels ++ labels.map(_.name).toSet
            recordType[R](node -> nodeType) >>
              updateTyping[R](node -> CTNode(detailed, qgn)) >>
              recordAndUpdate[R](expr -> CTBoolean)

          case x =>
            error(InvalidType(node, CTNode, x))
        }
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

    case Ors(exprs) =>
      for {
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
          // TODO: We can know for some cases that this is null, statically -- teach the planner to use that
          recordTypes(cmp.lhs -> lhsType, cmp.rhs -> rhsType) >> recordAndUpdate(expr -> CTBoolean.nullable)
        }
      } yield result

    case In(lhs, rhs) =>
      for {
        lhsType <- process[R](lhs)
        rhsType <- process[R](rhs)
        result <- rhsType match {
          case _: CTList | _: CTListOrNull | CTNull =>
            recordTypes(lhs -> lhsType, rhs -> rhsType) >> recordAndUpdate(expr -> CTBoolean)
          case x         =>
            error(InvalidType(rhs, CTList(CTWildcard), x))
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

    // TODO: Remove as soon as FunctionInvocationTyper is capable of supporting all Neo4j front end types
    case expr: FunctionInvocation if expr.function == ToString =>
      expr.arguments match {
        case Seq(first) =>
          for {
            inner <- process[R](first)
            existsType <- recordAndUpdate(expr -> CTString.asNullableAs(inner))
          } yield existsType
        case seq =>
          error(WrongNumberOfArguments(expr, 1, seq.size))
      }

    case expr: FunctionInvocation if expr.function == Collect =>
      for {
        argExprs <- pure(expr.arguments)
        argTypes <- argExprs.toList.traverse(process[R])
        computedType <- argTypes.reduceLeftOption(_ join _) match {
          case Some(innerType) =>
            pure[R, CypherType](CTList(innerType.nullable).nullable)
          case None =>
            error(NoSuitableSignatureForExpr(expr, argTypes))
        }
        result <- recordAndUpdate(expr -> computedType)
      } yield result

    case expr: FunctionInvocation if expr.function == Coalesce =>
      for {
        argExprs <- pure(expr.arguments)
        argTypes <- argExprs.toList.traverse(process[R])
        computedType <- {
          val relevantArguments = argTypes.indexWhere(!_.isNullable) match {
            case -1 => argTypes
            case other => argTypes.slice(0, other + 1)
          }
          relevantArguments.reduceLeftOption(_ join _) match {
            case Some(innerType) =>
              pure[R, CypherType](innerType)
            case None =>
              error(WrongNumberOfArguments(expr, 1, argTypes.size))
          }
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

    case indexing @ ContainerIndex(list @ ListLiteral(exprs), index: SignedDecimalIntegerLiteral)
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
        result <- recordAndUpdate(indexing -> eltType)
      } yield result

    case indexing @ ContainerIndex(list, index) =>
      for {
        listTyp <- process[R](list)
        indexTyp <- process[R](index)
        result <- (listTyp, indexTyp.material) match {

          // TODO: Test all cases
          case (CTList(eltTyp), CTInteger) =>
            recordAndUpdate(expr -> eltTyp.asNullableAs(indexTyp join eltTyp))

          case (CTListOrNull(eltTyp), CTInteger) =>
            recordAndUpdate(expr -> eltTyp.nullable)

          case (typ, CTString) if typ.subTypeOf(CTMap).maybeTrue =>
            recordAndUpdate(expr -> CTWildcard.nullable)

          case _ =>
            error(InvalidContainerAccess(indexing))
        }
      } yield result

    // generic CASE case (https://neo4j.com/docs/developer-manual/current/cypher/syntax/expressions/#query-syntax-case)
    case expr@ CaseExpression(None, alternatives, default) =>
      for {
        // get types for predicates of each alternative
        _ <- alternatives.map(_._1).toVector.traverse(process[R])
        alternativeType <- Foldable[Vector].foldMap(alternatives.map(_._2).toVector)(process[R])(joinMonoid)
        defaultType <- default match {
          case Some(expression) => process[R](expression)
          case None => pure[R, CypherType](alternativeType)
        }
        result <- recordAndUpdate(expr -> alternativeType.join(defaultType))
      } yield result

    case _ =>
      error(UnsupportedExpr(expr))
  }

  private def processArithmeticExpressions[R: _hasSchema: _keepsErrors: _hasTracker: _logsTypes](
      expr: Expression with BinaryOperatorExpression): Eff[R, CypherType] = {
    for {
      lhsType <- process[R](expr.lhs)
      rhsType <- process[R](expr.rhs)
      result <- {
        numericTypeOrError(expr, lhsType, rhsType) match {
          case Right(t) =>
            val typ = if (lhsType.isNullable || rhsType.isNullable) t.nullable else t
            recordTypes(expr.lhs -> lhsType, expr.rhs -> rhsType) >> recordAndUpdate(expr -> typ)
          case Left((e, t)) =>
            recordTypes(expr.lhs -> lhsType, expr.rhs -> rhsType) >> error(
              InvalidType(e, Seq(CTInteger, CTFloat, CTNumber), t))
        }
      }
    } yield result
  }

  private def numericTypeOrError(
      expr: Expression with BinaryOperatorExpression,
      lhsType: CypherType,
      rhsType: CypherType) = (lhsType.material, rhsType.material) match {
    case (CTInteger, CTInteger)                        => Right(CTInteger)
    case (CTFloat, CTInteger)                          => Right(CTFloat)
    case (CTInteger, CTFloat)                          => Right(CTFloat)
    case (CTFloat, CTFloat)                            => Right(CTFloat)
    case (CTNumber, y) if y.subTypeOf(CTNumber).isTrue => Right(CTNumber)
    case (x, CTNumber) if x.subTypeOf(CTNumber).isTrue => Right(CTNumber)
    case (x, _) if !x.couldBeSameTypeAs(CTNumber)      => Left(expr.lhs -> lhsType)
    case (_, y) if !y.couldBeSameTypeAs(CTNumber)      => Left(expr.rhs -> rhsType)
    case _                                             => Right(CTAny)
  }

  private def processAndsOrs[R: _hasSchema: _keepsErrors: _hasTracker: _logsTypes](
      expr: Expression,
      orderedExprs: Vector[Expression]): Eff[R, CypherType] = {
    for {
        // We have to process label predicates first in order to use label information on node
        // variables for looking up the type of property expressions on these variables.
      hasLabelPreds <- orderedExprs.collect { case h: HasLabels => h }.traverse(process[R])
      otherTypes <- orderedExprs.filter {
        case h: HasLabels => false
        case _ => true
      }.traverse(process[R])
      result <- {
        val innerTypes = hasLabelPreds ++ otherTypes
        val typeErrors = innerTypes.zipWithIndex.collect {
          case (t, idx) if t.material != CTBoolean =>
            error(InvalidType(orderedExprs(idx), CTBoolean, t))
        }
        val outType = if (innerTypes.exists(_.isNullable)) CTBoolean.nullable else CTBoolean
        if (typeErrors.isEmpty) {
          recordTypes(orderedExprs.zip(innerTypes): _*) >>
            recordAndUpdate(expr -> outType)
        } else typeErrors.reduce(_ >> _)
      }
    } yield result
  }

  private implicit class LiftedMonoid[R, A](val monoid: Monoid[A]) extends AnyVal with Monoid[Eff[R, A]] {
    override def empty: Eff[R, A] =
      pure(monoid.empty)

    override def combine(x: Eff[R, A], y: Eff[R, A]): Eff[R, A] =
      for { xTyp <- x; yTyp <- y } yield monoid.combine(xTyp, yTyp)
  }

  private sealed trait ExpressionTyper[T <: Expression] {
    def apply[R: _hasSchema: _keepsErrors: _hasTracker: _logsTypes](expr: T): Eff[R, CypherType]
  }

  private sealed trait SignatureBasedInvocationTyper[T <: Expression] extends ExpressionTyper[T] {

    def apply[R: _hasSchema: _keepsErrors: _hasTracker: _logsTypes](expr: T): Eff[R, CypherType] =
      for {
        argExprs <- pure(expr.arguments)
        argTypes <- argExprs.toList.traverse(process[R])
        arguments <- pure(argExprs.zip(argTypes))
        signatures <- selectSignaturesFor(expr, arguments)
        computedType <- signatures.map(_._2).reduceLeftOption(_ join _) match {
          case Some(outputType) =>
            pure[R, CypherType](if (argTypes.exists(_.isNullable)) outputType.nullable else outputType)

          case None =>
            error(NoSuitableSignatureForExpr(expr, argTypes))
        }
        resultType <- recordAndUpdate(expr -> computedType)
      } yield resultType

    protected def selectSignaturesFor[R: _hasSchema: _keepsErrors: _hasTracker: _logsTypes](
        expr: T,
        args: Seq[(Expression, CypherType)]): Eff[R, Set[(Seq[CypherType], CypherType)]] =
      for {
        signatures <- generateSignaturesFor(expr, args)
        eligible <- pure(signatures.flatMap { sig =>
          val (sigInputTypes, sigOutputType) = sig

          def compatibleArity = sigInputTypes.size == args.size
          def sigArgTypes = sigInputTypes.zip(args.map(_._2))
          def compatibleTypes = sigArgTypes.forall(((_: CypherType) couldBeSameTypeAs (_: CypherType)).tupled)

          if (compatibleArity && compatibleTypes) Some(sigInputTypes -> sigOutputType) else None
        })
      } yield eligible

    protected def generateSignaturesFor[R: _hasSchema: _keepsErrors: _hasTracker: _logsTypes](
        expr: T,
        args: Seq[(Expression, CypherType)]): Eff[R, Set[(Seq[CypherType], CypherType)]]
  }

  private case object FunctionInvocationTyper extends SignatureBasedInvocationTyper[FunctionInvocation] {
    override protected def generateSignaturesFor[R: _hasSchema: _keepsErrors: _hasTracker: _logsTypes](
        expr: FunctionInvocation,
        args: Seq[(Expression, CypherType)]): Eff[R, Set[(Seq[CypherType], CypherType)]] =
      expr.function match {
        case f: TypeSignatures =>
          pure(f.signatures.map { sig =>
            val sigInputTypes = sig.argumentTypes.map(fromFrontendType).map(_.nullable)
            val sigOutputType = fromFrontendType(sig.outputType)
            sigInputTypes -> sigOutputType
          }.toSet)

        case Min | Max =>
          pure[R, Set[(Seq[CypherType], CypherType)]](
            Set(
              Seq(CTInteger) -> CTInteger,
              Seq(CTFloat) -> CTFloat
            ))

        case ToBoolean =>
          pure[R, Set[(Seq[CypherType], CypherType)]](
            Set(
              Seq(CTString) -> CTBoolean,
              Seq(CTBoolean) -> CTBoolean
            ))

        case _ =>
          wrong[R, TyperError](UnsupportedExpr(expr)) >> pure(Set.empty)
      }
  }

  private case object AddTyper extends SignatureBasedInvocationTyper[Add] {
    override protected def generateSignaturesFor[R: _hasSchema: _keepsErrors: _hasTracker: _logsTypes](
        expr: Add,
        args: Seq[(Expression, CypherType)]): Eff[R, Set[(Seq[CypherType], CypherType)]] = {
      val (_, left) = args.head
      val (_, right) = args(1)
      (left.material -> right.material match {
        case (left: CTList, _)                                    => Some(left listConcatJoin right)
        case (_, right: CTList)                                   => Some(right listConcatJoin left)
        case (CTString, _) if right.subTypeOf(CTNumber).maybeTrue => Some(CTString)
        case (_, CTString) if left.subTypeOf(CTNumber).maybeTrue  => Some(CTString)
        case (CTString, CTString)                                 => Some(CTString)
        case (l, r) =>
          numericTypeOrError(expr, l, r) match {
            case Right(t) => Some(t)
            case _        => None
          }
      }) match {
        case Some(CTVoid) => wrong[R, TyperError](UnsupportedExpr(expr)) >> pure(Set(Seq(left, right) -> CTVoid))
        case Some(t)      => pure(Set(Seq(left, right) -> t.asNullableAs(left join right)))
        case None         => pure(Set.empty)
      }
    }

    private implicit class RichCTList(val left: CTList) extends AnyVal {
      def listConcatJoin(right: CypherType): CypherType = (left, right) match {
        case (CTList(lInner), CTList(rInner)) => CTList(lInner join rInner)
        case (CTList(CTString), CTString)     => left
        case (CTList(CTInteger), CTInteger)   => left
        case (CTList(CTFloat), CTInteger)     => CTList(CTNumber)
        case (CTList(CTVoid), _)              => CTList(right)
        case _                                => CTVoid
      }
    }
  }
}
