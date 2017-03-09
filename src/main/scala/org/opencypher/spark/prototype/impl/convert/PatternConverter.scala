package org.opencypher.spark.prototype.impl.convert

import cats._
import cats.data.State
import cats.data.State._
import cats.instances.list._
import cats.syntax.flatMap._
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection._
import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.opencypher.spark.prototype.api.expr.Expr
import org.opencypher.spark.prototype.api.ir._
import org.opencypher.spark.prototype.api.ir.global.{GlobalsRegistry, RelTypeRef}
import org.opencypher.spark.prototype.api.ir.pattern._

import scala.annotation.tailrec

final class PatternConverter(val tokens: GlobalsRegistry) extends AnyVal {

  type Result[A] = State[Pattern[Expr], A]

  def convert(p: ast.Pattern, pattern: Pattern[Expr] = Pattern.empty): Pattern[Expr] =
    convertPattern(p).runS(pattern).value

  private def convertPattern(p: ast.Pattern): Result[Unit] =
    Foldable[List].sequence_[Result, Unit](p.patternParts.toList.map(convertPart))

  @tailrec
  private def convertPart(p: ast.PatternPart): Result[Unit] = p match {
    case _: ast.AnonymousPatternPart => stomp(convertElement(p.element))
    case ast.NamedPatternPart(_, part) => convertPart(part)
  }

  private def convertElement(p: ast.PatternElement): Result[Field] = p match {
    case ast.NodePattern(Some(v), labels, None) => for {
        entity <- pure(Field(v.name))
        _ <- modify[Pattern[Expr]](_.withEntity(entity, EveryNode(AllGiven(labels.map(l => tokens.label(l.name)).toSet))))
      } yield entity

    case ast.RelationshipChain(left, ast.RelationshipPattern(Some(eVar), types, None, None, dir), right) => for {
      source <- convertElement(left)
      target <- convertElement(right)
      rel <- pure(Field(eVar.name))
      _ <- modify[Pattern[Expr]] { given =>
        val typeRefs =
          if (types.isEmpty) AnyGiven[RelTypeRef]()
          else AnyGiven[RelTypeRef](types.map(t => tokens.relType(t.name)).toSet)
        val registered = given.withEntity(rel, EveryRelationship(typeRefs))

        Endpoints.apply(source, target) match {
          case ends: IdenticalEndpoints =>
            registered.withConnection(rel, CyclicRelationship(ends))

          case ends: DifferentEndpoints =>
            dir match {
              case OUTGOING =>
                registered.withConnection(rel, DirectedRelationship(ends))

              case INCOMING =>
                registered.withConnection(rel, DirectedRelationship(ends.flip))

              case BOTH =>
                registered.withConnection(rel, UndirectedRelationship(ends))
            }
        }
      }
    } yield target
  }

  private def stomp[T](result: Result[T]): Result[Unit] = result >> pure(())
}
