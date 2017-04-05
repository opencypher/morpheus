package org.opencypher.spark.impl.types

import java.lang.Integer.parseInt

import cats.data._
import cats.implicits._
import cats.{Foldable, Monoid}
import org.atnos.eff._
import org.atnos.eff.all._
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.opencypher.spark.api.CypherType
import org.opencypher.spark.api.CypherType.joinMonoid
import org.opencypher.spark.prototype.api.schema.Schema
import org.opencypher.spark.api.types._

import scala.util.Try

/*
  TODO:

  * [X] Property Lookup
  * [X] Some basic literals
  * [X] List literals
  * [X] Function application, esp. considering overloading
  * [ ] Some operators: +, [], unary minus, AND
  * [ ] Stuff which messes with scope
  *
  * [ ] Dealing with same expression in multiple scopes
  * [ ] Make sure to always infer all implied labels
  * [ ] Actually using the schema to get list of slots
  *
  * [ ] Change type system to support union types (?)
 */
final case class SchemaTyper(schema: Schema) {

  def infer(expr: Expression, ctx: TyperContext = TyperContext.empty)
  : Either[NonEmptyList[TyperError], TyperResult[CypherType]] =
    SchemaTyper.processInContext[TyperStack[CypherType]](expr, ctx).run(schema)

  def inferOrThrow(expr: Expression, ctx: TyperContext = TyperContext.empty)
  : TyperResult[CypherType] =
    SchemaTyper.processInContext[TyperStack[CypherType]](expr, ctx).runOrThrow(schema)
}

object SchemaTyper {

  def processInContext[R: _hasSchema : _mayFail : _hasContext](expr: Expression, ctx: TyperContext)
  : Eff[R, CypherType] = for {
    _ <- put[R, TyperContext](ctx)
    result <- process[R](expr)
  } yield result

  def process[R: _hasSchema : _mayFail : _hasContext](expr: Expression): Eff[R, CypherType] = expr match {

    case _: Variable | _: Parameter =>
      typeOf[R](expr)

    case Property(v, PropertyKeyName(name)) =>
      for {
        varTyp <- process[R](v)
        schema <- ask[R, Schema]
        result <- varTyp.material match {
          case CTNode(labels) =>
            val keys = labels.collect { case (k, true) => k }.map(schema.nodeKeys).reduce(_ ++ _)
            updateTyping(expr -> keys.getOrElse(name, CTVoid).asNullableAs(varTyp))

          case CTRelationship(types) =>
            val keys = types.map(schema.relationshipKeys).reduce(_ ++ _)
            updateTyping(expr -> keys.getOrElse(name, CTVoid).asNullableAs(varTyp))

          case CTMap =>
            updateTyping(expr -> CTWildcard)

          case _ =>
            error(InvalidContainerAccess(expr))
        }
      } yield result

    case HasLabels(node, labels) =>
      for {
        nodeType <- process[R](node)
        result <- nodeType.material match {
          // TODO: Consider labels vs labels here
          case CTNode(nodeLabels) =>
            updateTyping(expr -> CTBoolean)

          case x =>
            error(InvalidType(node, CTNode, x))
        }
      } yield result

    case Not(inner) =>
      for {
        innerType <- process[R](inner)
        result <- innerType.material match {
          case CTBoolean =>
            updateTyping(expr -> CTBoolean)

          case x =>
            error(InvalidType(inner, CTBoolean, x))
        }
      } yield result

    case _: SignedDecimalIntegerLiteral =>
      updateTyping(expr -> CTInteger)

    case _: DecimalDoubleLiteral =>
      updateTyping(expr -> CTFloat)

    case _: BooleanLiteral =>
      updateTyping(expr -> CTBoolean)

    case _: StringLiteral =>
      updateTyping(expr -> CTString)

    case _: Null =>
      updateTyping(expr -> CTNull)

    case ListLiteral(elts) =>
      for {
        eltType <- Foldable[Vector].foldMap(elts.toVector)(process[R])(joinMonoid)
        listTyp <- updateTyping(expr -> CTList(eltType))
      } yield listTyp

    case expr: FunctionInvocation =>
      FunctionInvocationTyper(expr)

    case expr: Add =>
      AddTyper(expr)

    // TODO: This would be better handled by having a type of heterogenous lists instead
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

  private implicit class LiftedMonoid[R, A](val monoid: Monoid[A]) extends AnyVal with Monoid[Eff[R, A]] {
    override def empty: Eff[R, A] =
      pure(monoid.empty)

    override def combine(x: Eff[R, A], y: Eff[R, A]): Eff[R, A] =
      for {xTyp <- x; yTyp <- y} yield monoid.combine(xTyp, yTyp)
  }

  private sealed trait ExpressionTyper[T <: Expression] {
    def apply[R : _hasSchema : _mayFail : _hasContext](expr: T): Eff[R, CypherType]
  }

  private sealed trait SignatureBasedInvocationTyper[T <: Expression] extends ExpressionTyper[T] {

    def apply[R : _hasSchema : _mayFail : _hasContext](expr: T): Eff[R, CypherType] =
      for {
        argExprs <- pure(expr.arguments)
        argTypes <- EffMonad.traverse(argExprs.toList)(process[R])
        arguments <- pure(argExprs.zip(argTypes))
        signatures <- selectSignaturesFor(expr, arguments)
        computedType <- signatures.map(_._2).reduceLeftOption(_ join _) match {
          case Some(outputType) =>
            pure[R, CypherType](if (argTypes.exists(_.isNullable)) outputType.nullable else outputType)

          case None =>
            error(NoSuitableSignatureForExpr(expr))
        }
        resultType <- updateTyping(expr -> computedType)
      }
      yield resultType

    protected def selectSignaturesFor[R : _hasSchema : _mayFail : _hasContext]
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

    protected def generateSignaturesFor[R : _hasSchema : _mayFail : _hasContext]
      (expr: T, args: Seq[(Expression, CypherType)])
    : Eff[R, Set[(Seq[CypherType], CypherType)]]
  }

  private case object FunctionInvocationTyper extends SignatureBasedInvocationTyper[FunctionInvocation] {
    override protected def generateSignaturesFor[R : _hasSchema : _mayFail : _hasContext]
      (expr: FunctionInvocation, args: Seq[(Expression, CypherType)])
    : Eff[R, Set[(Seq[CypherType], CypherType)]] =
      expr.function match {
        case f: SimpleTypedFunction =>
          pure(f.signatures.map { sig =>
            val sigInputTypes = sig.argumentTypes.map(toCosType)
            val sigOutputType = toCosType(sig.outputType)
            sigInputTypes -> sigOutputType
          }.toSet)

        case _ =>
          wrong[R, TyperError](UnsupportedExpr(expr)) >> pure(Set.empty)
      }
  }

  private case object AddTyper extends SignatureBasedInvocationTyper[Add] {
    override protected def generateSignaturesFor[R : _hasSchema : _mayFail : _hasContext]
    (expr: Add, args: Seq[(Expression, CypherType)])
    : Eff[R, Set[(Seq[CypherType], CypherType)]] = {
      val (_, left) = args.head
      val (_, right) = args(1)
      val outTyp = left.material -> right.material match {
        case (left: CTList, _) => left listConcatJoin right
        case (_, right: CTList) => right listConcatJoin left
        case (CTInteger, CTFloat) => CTFloat
        case (CTFloat, CTInteger) => CTFloat
        case (CTString, _) if right.subTypeOf(CTNumber).maybeTrue => CTString
        case (_, CTString) if left.subTypeOf(CTNumber).maybeTrue => CTString
        case _  => left join right
      }
      pure(Set(Seq(left, right) -> outTyp.asNullableAs(left join right)))
    }

      private implicit class RichCTList(val left: CTList) extends AnyVal {
        def listConcatJoin(right: CypherType): CypherType = (left, right) match {
          case (CTList(lInner), CTList(rInner)) => CTList(lInner join rInner)
          case (CTList(CTString), CTString) => left
          case (CTList(CTInteger), CTInteger) => left
          case (CTList(CTFloat), CTInteger) => CTList(CTNumber)
          case (CTList(CTVoid), _) => CTList(right)
          // TODO: Throw type error instead
          case _ => CTVoid
        }
      }
  }
}


