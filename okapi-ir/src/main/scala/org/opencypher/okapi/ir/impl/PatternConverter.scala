/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.okapi.ir.impl

import cats._
import cats.data.State
import cats.data.State._
import cats.instances.list._
import cats.syntax.flatMap._
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, LogicalVariable, RelTypeName, RelationshipChain}
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v3_4.{expressions => ast}
import org.opencypher.okapi.api.types.{CTList, CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.api.util.FreshVariableNamer

import scala.annotation.tailrec

final class PatternConverter {

  type Result[A] = State[Pattern[Expr], A]

  def convert(
      p: ast.Pattern,
      knownTypes: Map[ast.Expression, CypherType],
      pattern: Pattern[Expr] = Pattern.empty): Pattern[Expr] =
    convertPattern(p, knownTypes).runS(pattern).value

  def convertRelsPattern(
      p: ast.RelationshipsPattern,
      knownTypes: Map[ast.Expression, CypherType],
      pattern: Pattern[Expr] = Pattern.empty): Pattern[Expr] =
    convertElement(p.element, knownTypes).runS(pattern).value

  private def convertPattern(p: ast.Pattern, knownTypes: Map[ast.Expression, CypherType]): Result[Unit] =
    Foldable[List].sequence_[Result, Unit](p.patternParts.toList.map(convertPart(knownTypes)))

  @tailrec
  private def convertPart(knownTypes: Map[ast.Expression, CypherType])(p: ast.PatternPart): Result[Unit] = p match {
    case _: ast.AnonymousPatternPart   => stomp(convertElement(p.element, knownTypes))
    case ast.NamedPatternPart(_, part) => convertPart(knownTypes)(part)
  }

  private def convertEquivalenceModel(m: ast.EquivalenceModel, knownTypes: Map[ast.Expression, CypherType], fallbackType: CypherType): EquivalenceModel = {
    m match {
      case ast.TildeModel(v) => TildeModel(Var(v.name)(knownTypes.get(v).getOrElse(fallbackType)))
      case ast.AtModel(v) => AtModel(Var(v.name)(knownTypes(v)))
    }
  }

  private def convertElement(p: ast.PatternElement, knownTypes: Map[ast.Expression, CypherType]): Result[IRField] =
    p match {

      case np @ ast.NodePattern(vOpt, labels: Seq[ast.LabelName], None, equivalenceModelOpt) =>
        // labels within CREATE patterns, e.g. CREATE (a:Foo), labels for MATCH clauses are rewritten to WHERE
        val patternLabels = labels.map(_.name).toSet

        // labels for ~ / @ are extracted from the referred variable
        val equivalence = equivalenceModelOpt.map(convertEquivalenceModel(_, knownTypes, CTNode(patternLabels)))
        val equivalenceLabels = equivalence.map(_.v.cypherType.asInstanceOf[CTNode].labels).getOrElse(Set.empty)

        // labels defined in outside scope, passed in by IRBuilder
        val knownLabels = vOpt.map(expr => knownTypes.get(expr) match {
          case Some(CTNode(l)) => l
          case _ => Set.empty
        }).getOrElse(Set.empty)

        val allLabels = patternLabels ++ equivalenceLabels ++ knownLabels

        val nodeVar = vOpt match {
          case Some(v) => Var(v.name)(CTNode(allLabels))
          case None    => FreshVariableNamer(np.position.offset, CTNode(allLabels))
        }
        for {
          entity <- pure(IRField(nodeVar.name)(nodeVar.cypherType))
          _ <- modify[Pattern[Expr]](_.withEntity(entity, equivalence))
        } yield entity

      case rc @ ast.RelationshipChain(left, ast.RelationshipPattern(eOpt, types, None, None, dir, equivalenceModelOpt, _), right) =>

        val rel = createRelationshipVar(knownTypes, rc.position.offset, eOpt, types, equivalenceModelOpt)

        for {
          source <- convertElement(left, knownTypes)
          target <- convertElement(right, knownTypes)
          rel <- pure(IRField(rel.name)(rel.cypherType))
          equivalence = equivalenceModelOpt.map(convertEquivalenceModel(_, knownTypes, rel.cypherType))
          _ <- modify[Pattern[Expr]] { given =>
            val registered = given.withEntity(rel, equivalence)

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

      case rc @ ast.RelationshipChain(
            left,
            ast.RelationshipPattern(eOpt, types, Some(Some(range)), None, dir, equivalenceModelOpt, _),
            right) =>

        val rel = createRelationshipVar(knownTypes, rc.position.offset, eOpt, types, equivalenceModelOpt)

        for {
          source <- convertElement(left, knownTypes)
          target <- convertElement(right, knownTypes)
          rel <- pure(IRField(rel.name)(CTList(rel.cypherType)))
          equivalence = equivalenceModelOpt.map(convertEquivalenceModel(_, knownTypes, rel.cypherType))
          _ <- modify[Pattern[Expr]] { given =>
            val registered = given.withEntity(rel, equivalence)

            // TODO: replace with match on range parameter and remove upper case
            val lower = range.lower.map(_.value.intValue()).getOrElse(1)
            val upper =
              range.upper
                .map(_.value.intValue())
                .getOrElse(throw NotImplementedException("Support for unbounded var-length not yet implemented"))

            Endpoints.apply(source, target) match {
              case _: IdenticalEndpoints =>
                throw NotImplementedException("Support for cyclic var-length not yet implemented")

              case ends: DifferentEndpoints =>
                dir match {
                  case OUTGOING =>
                    registered.withConnection(rel, DirectedVarLengthRelationship(ends, lower, Some(upper)))

                  case INCOMING =>
                    registered.withConnection(rel, DirectedVarLengthRelationship(ends.flip, lower, Some(upper)))

                  case BOTH =>
                    registered.withConnection(rel, UndirectedVarLengthRelationship(ends.flip, lower, Some(upper)))
                }
            }
          }
        } yield target

      case x =>
        throw NotImplementedException(s"Support for pattern conversion of $x not yet implemented")
    }

  private def createRelationshipVar(
    knownTypes: Map[Expression, CypherType],
    offset: Int,
    eOpt: Option[LogicalVariable],
    types: Seq[RelTypeName],
    equivalenceModelOpt: Option[ast.EquivalenceModel]): Var = {

    val patternTypes = types.map(_.name).toSet

    // types for ~ / @ are extracted from the referred variable
    val equivalence = equivalenceModelOpt.map(convertEquivalenceModel(_, knownTypes, CTRelationship(patternTypes)))
    val equivalenceTypes = equivalence.map(_.v.cypherType.asInstanceOf[CTRelationship].types).getOrElse(Set.empty)

    // types defined in outside scope, passed in by IRBuilder
    val knownRelTypes = eOpt.map(expr => knownTypes.get(expr) match {
      case Some(CTRelationship(t)) => t
      case _ => Set.empty
    }).getOrElse(Set.empty)

    val relTypes = patternTypes ++ equivalenceTypes ++ knownRelTypes

    val rel = eOpt match {
      case Some(v) => Var(v.name)(CTRelationship(relTypes))
      case None => FreshVariableNamer(offset, CTRelationship(relTypes))
    }
    rel
  }

  private def stomp[T](result: Result[T]): Result[Unit] = result >> pure(())
}
