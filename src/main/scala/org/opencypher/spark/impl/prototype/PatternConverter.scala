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
    case ast.NodePattern(Some(v), labels, None) => for {
        entity <- pure(Field(v.name))
        _ <- modify[Given](_.withEntity(entity, AnyNode(WithEvery(labels.map(l => tokens.label(l.name)).toSet))))
      } yield entity

    case ast.RelationshipChain(left, ast.RelationshipPattern(Some(eVar), types, None, None, dir), right) => for {
      source <- convertElement(left)
      target <- convertElement(right)
      rel <- pure(Field(eVar.name))
      _ <- modify[Given] { given =>
        val typeRefs =
          if (types.isEmpty) WithAny.empty[RelTypeRef]
          else WithAny[RelTypeRef](types.map(t => tokens.relType(t.name)).toSet)
        val registered = given.withEntity(rel, AnyRelationship(typeRefs))

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
                registered
                  .withConnection(rel, DirectedRelationship(ends))
                  .withConnection(rel, DirectedRelationship(ends.flip))
            }
        }
      }
    } yield target
  }

  private def stomp[T](result: Result[T]): Result[Unit] = result >> pure(())
}
