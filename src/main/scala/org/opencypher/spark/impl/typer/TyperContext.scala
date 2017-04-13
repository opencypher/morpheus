package org.opencypher.spark.impl.typer

import cats.syntax.all._
import org.atnos.eff._
import org.atnos.eff.all._
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Expression, Parameter}
import org.neo4j.cypher.internal.frontend.v3_2.symbols
import org.opencypher.spark.api.types._

object TyperContext2 {
  def empty = TyperContext2(TypeTracker.empty)
}

final case class TyperContext2(tracker: TypeTracker) {

//  def :+(entry: (Expression, CypherType)): TyperContext = {
//    val (expr, typ) = entry
//    expr match {
//      case param: Parameter =>
//        val realTyp = typ meet fromFrontendType(param.parameterType)
//        copy(typings = typings
//          .updated(expr, realTyp)
//          .updated(param.copy(parameterType = symbols.CTAny)(param.position), realTyp)
//        )
//
//      case _ =>
//        copy(typings = typings.updated(expr, typ))
//    }
//  }
//
//  def getTypeOf[R : _keepsErrors : _hasContext](it: Expression): Eff[R, CypherType] =
//    typings.get(it).map(pure[R, CypherType]).getOrElse(error(UnTypedExpr(it)))
//
//  def putUpdated[R : _keepsErrors : _hasContext](entry: (Expression, CypherType)): Eff[R, CypherType] =
//    for {
//      context <- get[R, TyperContext]
//      result <- {
//        val (ref, newTyp) = entry
//        typings.get(ref) match {
//          case Some(oldTyp) if oldTyp == newTyp =>
//            pure[R, CypherType](oldTyp)
//
//          case Some(oldTyp) if detailingEntityType(oldTyp, newTyp) =>
//            if (context.atTopLevel)
//              put[R, TyperContext](copy(typings = typings.updated(ref, newTyp))) >> pure[R, CypherType](newTyp)
//            else
//              pure[R, CypherType](oldTyp)
//
//          case Some(oldTyp) =>
//            put[R, TyperContext](copy(typings = typings.updated(ref, CTWildcard))) >>
//              error[R](AlreadyTypedExpr(ref, oldTyp, newTyp))
//
//          case None =>
//            put[R, TyperContext](copy(typings = typings.updated(ref, newTyp))) >> pure[R, CypherType](newTyp)
//        }
//      }
//    } yield result
//
//  private def detailingEntityType(old: CypherType, newT: CypherType): Boolean = (old, newT) match {
//    case (o: CTNode, n: CTNode) if (o superTypeOf n).isTrue => true
//    case (o: CTRelationship, n: CTRelationship) if (o superTypeOf n).isTrue => true
//    case _ => false
//  }
}
