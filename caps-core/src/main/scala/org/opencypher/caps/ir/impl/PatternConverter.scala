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
package org.opencypher.caps.ir.impl

import cats._
import cats.data.State
import cats.data.State._
import cats.instances.list._
import cats.syntax.flatMap._
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection._
import org.neo4j.cypher.internal.frontend.v3_3.ast
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, LabelName}
import org.opencypher.caps.api.expr.Expr
import org.opencypher.caps.api.types.{CTList, CTNode, CTRelationship, CypherType}
import org.opencypher.caps.api.value.CypherValue
import org.opencypher.caps.impl.exception.Raise
import org.opencypher.caps.ir.api._
import org.opencypher.caps.ir.api.pattern._

import scala.annotation.tailrec

final class PatternConverter(val parameters: Map[String, CypherValue]) extends AnyVal {

  type Result[A] = State[Pattern[Expr], A]

  def convert(p: ast.Pattern, knownTypes: Map[Expression, CypherType], pattern: Pattern[Expr] = Pattern.empty): Pattern[Expr] =
    convertPattern(p, knownTypes).runS(pattern).value

  private def convertPattern(p: ast.Pattern, knownTypes: Map[Expression, CypherType]): Result[Unit] =
    Foldable[List].sequence_[Result, Unit](p.patternParts.toList.map(convertPart(knownTypes)))

  @tailrec
  private def convertPart(knownTypes: Map[Expression, CypherType])(p: ast.PatternPart): Result[Unit] = p match {
    case _: ast.AnonymousPatternPart => stomp(convertElement(p.element, knownTypes))
    case ast.NamedPatternPart(_, part) => convertPart(knownTypes)(part)
  }

  private def convertElement(p: ast.PatternElement, knownTypes: Map[Expression, CypherType]): Result[IRField] = p match {
    case ast.NodePattern(Some(v), labels: Seq[LabelName], None) =>
      for {
        entity <- pure(IRField(v.name)(knownTypes.getOrElse(v, CTNode(labels.map(_.name).toSet))))
        _ <- modify[Pattern[Expr]](_.withEntity(entity))
      } yield entity

    case ast.RelationshipChain(left, ast.RelationshipPattern(Some(eVar), types, None, None, dir, _), right) =>
      for {
        source <- convertElement(left, knownTypes)
        target <- convertElement(right, knownTypes)
        rel <- pure(IRField(eVar.name)(knownTypes.getOrElse(eVar, CTRelationship(types.map(_.name).toSet))))
        _ <- modify[Pattern[Expr]] { given =>
          val registered = given.withEntity(rel)

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

    case ast.RelationshipChain(left, ast.RelationshipPattern(Some(eVar), types, Some(Some(range)), None, dir, _), right) =>
      for {
        source <- convertElement(left, knownTypes)
        target <- convertElement(right, knownTypes)
        rel <- pure(IRField(eVar.name)(CTList(CTRelationship(types.map(_.name).toSet))))
        _ <- modify[Pattern[Expr]] { given =>
          val registered = given.withEntity(rel)

          val lower = range.lower.map(_.value.intValue()).getOrElse(1)
          val upper = range.upper.map(_.value.intValue()).getOrElse(Raise.notYetImplemented("unbounded variable length"))

          Endpoints.apply(source, target) match {
            case _: IdenticalEndpoints =>
              Raise.notYetImplemented("cyclic var-length")

            case ends: DifferentEndpoints =>
              dir match {
                case OUTGOING =>
                  registered.withConnection(rel, DirectedVarLengthRelationship(ends, lower, Some(upper)))

                case INCOMING =>
                  registered.withConnection(rel, DirectedVarLengthRelationship(ends.flip, lower, Some(upper)))

                case BOTH =>
                  Raise.notYetImplemented("undirected var-length")
              }
          }
        }
      } yield target

    case x =>
      Raise.notYetImplemented(s"pattern conversion of $x")
  }

  private def stomp[T](result: Result[T]): Result[Unit] = result >> pure(())
}
