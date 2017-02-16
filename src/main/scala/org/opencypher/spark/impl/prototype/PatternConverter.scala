package org.opencypher.spark.impl.prototype

import cats._
import cats.data.State
import cats.data.State._
import cats.instances.list._
import cats.syntax.flatMap._
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection._
import org.neo4j.cypher.internal.frontend.v3_2.ast

import scala.annotation.tailrec

final class PatternConverter(val tokens: TokenDefs) extends AnyVal {

  type Result[A] = State[Given, A]

  def convert(p: ast.Pattern, given: Given = Given.nothing): Given =
    convertPattern(p).runS(given).value

  private def convertPattern(p: ast.Pattern): Result[Unit] =
    Foldable[List].sequence_[Result, Unit](p.patternParts.toList.map(convertPart))

  @tailrec
  private def convertPart(p: ast.PatternPart): Result[Unit] = p match {
    case _: ast.AnonymousPatternPart => stomp(convertElement(p.element))
    case ast.NamedPatternPart(_, part) => convertPart(part)
  }

  private def convertElement(p: ast.PatternElement): Result[Field] = p match {
    case ast.NodePattern(Some(v), _, None) => for {
        entity <- pure(Field(v.name))
        _ <- modify[Given](_ + AnyNode(entity))
      } yield entity

    case ast.RelationshipChain(left, ast.RelationshipPattern(Some(eVar), types, None, None, dir), right) => for {
      from <- convertElement(left)
      to <- convertElement(right)
      entity <- pure(Field(eVar.name))
      _ <- modify[Given] { given =>
        val newTypes = if (types.isEmpty) Seq(None) else types.map(t => Some(tokens.relType(t.name)))
        val newEntities = dir match {
          case OUTGOING =>
            newTypes.map(t => AnyRelationship(from, entity, to, t))

          case INCOMING =>
            newTypes.map(t => AnyRelationship(to, entity, from, t))

          case BOTH =>
            newTypes.flatMap(t => Seq(
              AnyRelationship(from, entity, to, t),
              AnyRelationship(to, entity, from, t))
            )
        }
        newEntities.foldLeft(given)(_ + _)
      }
    } yield to
  }

  private def stomp[T](result: Result[T]): Result[Unit] = result >> pure(())
}
