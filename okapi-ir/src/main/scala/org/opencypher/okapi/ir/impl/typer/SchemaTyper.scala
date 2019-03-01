/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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

import cats.data._
import cats.implicits._
import cats.{Foldable, Monoid}
import org.atnos.eff._
import org.atnos.eff.all.{get, _}
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CypherType.joinMonoid
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.ir.impl.parse.functions.FunctionExtensions
import org.opencypher.okapi.ir.impl.parse.rewriter.ExistsPattern
import org.opencypher.okapi.ir.impl.typer.SignatureConverter._
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions.{Coalesce, Collect, Exists, Properties}

final case class SchemaTyper(schema: Schema) {

  def infer(
    expr: Expression,
    tracker: TypeTracker = TypeTracker.empty
  ): Either[NonEmptyList[TyperError], TyperResult[CypherType]] =
    SchemaTyper.processInContext[TyperStack[CypherType]](expr, tracker).run(schema)

  def inferOrThrow(expr: Expression, tracker: TypeTracker = TypeTracker.empty): TyperResult[CypherType] =
    SchemaTyper.processInContext[TyperStack[CypherType]](expr, tracker).runOrThrow(schema)
}

object SchemaTyper {

  def processInContext[R: _hasSchema : _keepsErrors : _hasTracker : _logsTypes](
    expr: Expression,
    tracker: TypeTracker
  ): Eff[R, CypherType] =
    for {
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
      for {
        varTyp <- process[R](v)
        schema <- ask[R, Schema]
        result <- varTyp.material match {

          case CTVoid =>
            recordAndUpdate(expr -> CTNull)

          // This means that the node can have any possible label combination, as the user did not specify any constraints
          case n: CTNode if n.labels.isEmpty =>
            val propType = schema.allCombinations
              .map(l => schema.nodePropertyKeyType(l, name).getOrElse(CTNull))
              .foldLeft(CTVoid: CypherType)(_ join _)
            recordType(v -> varTyp) >> recordAndUpdate(expr -> propType)

          // User specified label constraints - we can use those for type inference
          case CTNode(labels, _) =>
            val propType = schema.nodePropertyKeyType(labels, name).getOrElse(CTNull)
            recordType(v -> varTyp) >> recordAndUpdate(expr -> propType)

          case CTRelationship(types, _) =>
            val propType = schema.relationshipPropertyKeyType(types, name).getOrElse(CTNull)
            recordType(v -> varTyp) >> recordAndUpdate(expr -> propType)

          case CTMap(inner) =>
            recordType(v -> varTyp) >> recordAndUpdate(expr -> inner.getOrElse(name, CTVoid))

          case _: TemporalValueCypherType =>
            recordType(v -> varTyp) >> recordAndUpdate(expr -> CTInteger.asNullableAs(varTyp))

          case _ =>
            error(InvalidContainerAccess(expr))
        }
      } yield result

    case MapExpression(items) =>
      val keys = items.map(_._1.name)
      val values = items.map(_._2)
      for {
        valueTypes <- values.toList.traverse(process[R])
        mapType <- recordAndUpdate(expr -> CTMap(keys.zip(valueTypes).toMap))
      } yield mapType

    case _: ExistsPattern =>
      recordAndUpdate[R](expr -> CTBoolean)

    case HasLabels(node, labels) =>
      for {
        nodeType <- process[R](node)
        result <- nodeType.material match {
          case CTVoid =>
            pure[R, CypherType](CTNull)

          case CTNode(nodeLabels, qgn) =>
            val detailed = nodeLabels ++ labels.map(_.name).toSet
            updateTyping[R](node -> CTNode(detailed, qgn)) >>
            pure[R, CypherType](CTBoolean)

          case x =>
            error(InvalidType(node, CTNode, x))
        }
        _ <- recordType[R](node -> nodeType)
        _ <- recordAndUpdate[R](expr -> result)
      } yield result

    case Not(inner) =>
      for {
        innerType <- process[R](inner)
        result <- innerType.material match {
          case CTBoolean | CTVoid =>
            recordType(inner -> innerType) >>
              recordAndUpdate(expr -> CTBoolean)

          case x =>
            error(InvalidType(inner, CTBoolean, x))
        }
      } yield result

    case Ands(exprs) => processAndsOrs(expr, exprs.toVector)

    case Ors(exprs) => processAndsOrs(expr, exprs.toVector)

    case Equals(lhs, rhs) =>
      for {
        lhsType <- process[R](lhs)
        rhsType <- process[R](rhs)
        result <- recordTypes(lhs -> lhsType, rhs -> rhsType) >> recordAndUpdate(expr -> CTBoolean.asNullableAs(lhsType join rhsType))
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
        result <- (lhsType, rhsType) match {
          case (_, CTNull | CTList(CTNull) | CTListOrNull(CTNull)) => pure[R, CypherType](CTNull)
          case (CTNull, CTList(CTVoid))                            => pure[R, CypherType](CTBoolean)
          case (CTNull, CTListOrNull(CTVoid))                      => pure[R, CypherType](CTBoolean.nullable)
          case (CTNull, CTList(_) | CTListOrNull(_))               => pure[R, CypherType](CTNull)
          case (m, CTList(e))                                      => pure[R, CypherType](CTBoolean.asNullableAs(m join e))
          case (_, CTListOrNull(_))                                => pure[R, CypherType](CTBoolean.nullable)
          case (r, _)                                              => error(InvalidType(rhs, CTList(CTAny), r))
        }
        _ <- recordTypes(lhs -> lhsType, rhs -> rhsType)
        _ <- recordAndUpdate(expr -> result)
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
            argType <- process[R](first)
            mapType <- process[R](first.map)
            existsType <- pure((mapType, argType) match {
              case (CTNull, _) => CTNull
              case _ => CTBoolean
            })
            _ <- recordAndUpdate(expr -> existsType)
          } yield existsType
        case Seq(nonProp) =>
          error(InvalidArgument(expr, nonProp))
        case seq =>
          error(WrongNumberOfArguments(expr, 1, seq.size))
      }

    case expr: FunctionInvocation if expr.function == Properties =>
      expr.arguments match {
        case Seq(first) =>
          for {
            inner <- process[R](first)
            outType <- inner match {
              case CTNull =>
                pure[R, CypherType](CTNull)
              case _ => for {
                schema <- ask[R, Schema]
                properties <- inner.material match {
                  case CTNode(labels, _) =>
                    pure[R, PropertyKeys](schema.nodePropertyKeysForCombinations(schema.combinationsFor(labels)))
                  case CTRelationship(types, _) =>
                    pure[R, PropertyKeys](schema.relationshipPropertyKeysForTypes(types))
                  case CTMap(properties) =>
                    pure[R, PropertyKeys](properties)
                  case _ =>
                    wrong[R, TyperError](InvalidArgument(expr, first)) >> pure[R, PropertyKeys](Map.empty)
                }
                mapType <- pure[R, CypherType](CTMap(properties).asNullableAs(inner))
              } yield mapType
            }
            _ <- recordAndUpdate(expr -> outType)
          } yield outType

        case seq =>
          error(WrongNumberOfArguments(expr, 1, seq.size))
      }

    case expr: FunctionInvocation if expr.function == Collect =>
      for {
        argExprs <- pure(expr.arguments)
        argTypes <- argExprs.toList.traverse(process[R])
        computedType <- argTypes.reduceLeftOption(_ join _) match {
          case Some(innerType) =>
            pure[R, CypherType](CTList(innerType).nullable)
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
      BasicSignatureBasedTyper(expr)

    case CountStar() =>
      recordAndUpdate(expr -> CTInteger)

    case add: Add =>
      AddTyper(add)

    case sub: Subtract =>
      BasicSignatureBasedTyper(sub)

    case mul: Multiply =>
      processArithmeticExpressions(mul)

    case div: Divide =>
      processArithmeticExpressions(div)

    case indexing@ContainerIndex(list, index) =>
      for {
        containerType <- process[R](list)
        indexTyp <- process[R](index)
        tracker <- get[R, TypeTracker]
        result <- (containerType.material, indexTyp.material) match {
          case (CTList(eltTyp), CTInteger) =>
            recordAndUpdate(expr -> eltTyp.nullable)

          case (_: CTList, keyType) =>
            error(InvalidType(index, CTInteger, keyType))

          case (CTMap(innerTypes), CTString) =>
            val valueType = index match {
              case Parameter(name, _) =>
                val key = tracker.parameters(name).cast[String]
                innerTypes.getOrElse(key, CTVoid)
              case StringLiteral(key) => innerTypes.getOrElse(key, CTVoid)
              case _ => innerTypes.values.reduce(_ join _).nullable
            }
            recordAndUpdate(expr -> (if (containerType.isNullable) valueType.nullable else valueType))

          case (_: CTMap, keyType) =>
            error(InvalidType(index, CTString, keyType))

          case _ =>
            error(InvalidContainerAccess(indexing))
        }
      } yield result

    // generic CASE case (https://neo4j.com/docs/developer-manual/current/cypher/syntax/expressions/#query-syntax-case)
    case expr@CaseExpression(None, alternatives, default) =>
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

    case operatorExpression: OperatorExpression =>
      BasicSignatureBasedTyper(operatorExpression)

    case _ =>
      error(UnsupportedExpr(expr))
  }

  private def processArithmeticExpressions[R: _hasSchema : _keepsErrors : _hasTracker : _logsTypes](
    expr: Expression with BinaryOperatorExpression
  ): Eff[R, CypherType] = {
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
    rhsType: CypherType
  ) = (lhsType.material, rhsType.material) match {
    case (CTVoid, _) => Right(CTNull)
    case (_, CTVoid) => Right(CTNull)
    case (CTInteger, CTInteger) => Right(CTInteger)
    case (CTFloat, CTInteger) => Right(CTFloat)
    case (CTInteger, CTFloat) => Right(CTFloat)
    case (CTFloat, CTFloat) => Right(CTFloat)
    case (CTNumber, y) if y.subTypeOf(CTNumber) => Right(CTNumber)
    case (x, CTNumber) if x.subTypeOf(CTNumber) => Right(CTNumber)
    case (x, _) if !x.couldBeSameTypeAs(CTNumber) => Left(expr.lhs -> lhsType)
    case (_, y) if !y.couldBeSameTypeAs(CTNumber) => Left(expr.rhs -> rhsType)
    case _ => Right(CTAny)
  }

  private def processAndsOrs[R: _hasSchema : _keepsErrors : _hasTracker : _logsTypes](
    expr: Expression,
    orderedExprs: Vector[Expression]
  ): Eff[R, CypherType] = {
    for {
      // We have to process label predicates first in order to use label information on node
      // variables for looking up the type of property expressions on these variables.
      hasLabelPreds <- orderedExprs.collect { case h: HasLabels => h }.traverse(process[R])
      otherTypes <- orderedExprs.filter {
        case _: HasLabels => false
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
      for {xTyp <- x; yTyp <- y} yield monoid.combine(xTyp, yTyp)
  }

  private sealed trait ExpressionTyper[T <: Expression] {
    def apply[R: _hasSchema : _keepsErrors : _hasTracker : _logsTypes](expr: T): Eff[R, CypherType]
  }

  private sealed trait SignatureBasedInvocationTyper[T <: Expression] extends ExpressionTyper[T] {

    def apply[R: _hasSchema : _keepsErrors : _hasTracker : _logsTypes](expr: T): Eff[R, CypherType] =
      for {
        argExprs <- pure(expr.arguments)
        argTypes <- argExprs.toList.traverse(process[R])
        arguments <- pure(argExprs.zip(argTypes))
        signatures <- generateSignaturesFor(expr, arguments)
        outTypes <- pure(signatures.flatMap { sig =>
          val sigInputTypes = sig.input
          val sigOutputType = sig.output
          val argTypes = arguments.map(_._2)

          def compatibleArity = sigInputTypes.size == arguments.size

          def compatibleTypes = sigInputTypes.zip(argTypes).forall {
            case (_: CTMap | _: CTMapOrNull, _: CTMap) => true
            case (sigType, argType) => sigType intersects argType
          }

          if (compatibleArity && compatibleTypes) Some(sigOutputType) else None
        })
        computedType <- outTypes.reduceLeftOption(_ join _) match {
          case Some(outputType) =>
            pure[R, CypherType](if (argTypes.exists(_.isNullable)) outputType.nullable else outputType)

          case None =>
            error(NoSuitableSignatureForExpr(expr, argTypes))
        }
        _ <- recordAndUpdate(expr -> computedType)
      } yield computedType

    protected def generateSignaturesFor[R: _hasSchema : _keepsErrors : _hasTracker : _logsTypes](
      expr: T,
      args: Seq[(Expression, CypherType)]
    ): Eff[R, Set[FunctionSignature]]
  }

  private case object BasicSignatureBasedTyper extends SignatureBasedInvocationTyper[Expression] {
    override protected def generateSignaturesFor[R: _hasSchema : _keepsErrors : _hasTracker : _logsTypes](
      expr: Expression,
      args: Seq[(Expression, CypherType)]
    ): Eff[R, Set[FunctionSignature]] = expr match {

      case f: FunctionInvocation =>

        val function =
          FunctionExtensions
            .get(f.name)
            .getOrElse(f.function)

        function match {

          case t: TypeSignatures => pure(
            SignatureConverter.from(t.signatures)
              .expandWithNulls
              .expandWithSubstitutions(CTFloat, CTInteger)
              .signatures
          )

          case _ =>
            wrong[R, TyperError](UnsupportedExpr(expr)) >> pure(Set.empty)
        }

      case o: OperatorExpression => pure(
        SignatureConverter.from(o.signatures)
          .expandWithNulls
          .expandWithSubstitutions(CTFloat, CTInteger)
          .signatures
      )

      case _ =>
        wrong[R, TyperError](UnsupportedExpr(expr)) >> pure(Set.empty)
    }
  }

  private case object AddTyper extends SignatureBasedInvocationTyper[Add] {
    override protected def generateSignaturesFor[R: _hasSchema : _keepsErrors : _hasTracker : _logsTypes](
      expr: Add,
      args: Seq[(Expression, CypherType)]
    ): Eff[R, Set[FunctionSignature]] = {
      val (_, left) = args.head
      val (_, right) = args(1)
      (left.material -> right.material match {
        case (left: CTList, _) => Some(left listConcatJoin right)
        case (_, right: CTList) => Some(right listConcatJoin left)
        case (CTString, _) if right.subTypeOf(CTNumber) => Some(CTString)
        case (_, CTString) if left.subTypeOf(CTNumber) => Some(CTString)
        case (CTString, CTString) => Some(CTString)
        case (CTDuration, CTDuration) => Some(CTDuration)
        case (CTLocalDateTime, CTDuration) => Some(CTLocalDateTime)
        case (CTDuration, CTLocalDateTime) => Some(CTLocalDateTime)
        case (CTDate, CTDuration) => Some(CTDate)
        case (CTDuration, CTDate) => Some(CTDate)
        case (l, r) =>
          numericTypeOrError(expr, l, r) match {
            case Right(t) => Some(t)
            case _ => None
          }
      }) match {
        case Some(CTVoid) => wrong[R, TyperError](UnsupportedExpr(expr)) >> pure(Set(FunctionSignature(Seq(left, right), CTVoid)))
        case Some(t) => pure(Set(FunctionSignature(Seq(left, right), t.asNullableAs(left join right))))
        case None => pure(Set.empty)
      }
    }

    private implicit class RichCTList(val left: CTList) extends AnyVal {
      def listConcatJoin(right: CypherType): CypherType = (left, right) match {
        case (CTList(lInner), CTList(rInner)) => CTList(lInner join rInner)
        case (CTList(lInner), _) => CTList(lInner join right)
      }
    }
  }
}
