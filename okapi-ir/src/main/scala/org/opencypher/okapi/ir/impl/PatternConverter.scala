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
package org.opencypher.okapi.ir.impl

import cats._
import cats.data.State
import cats.data.State._
import cats.instances.list._
import cats.syntax.flatMap._
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, LogicalVariable, RelTypeName}
import org.neo4j.cypher.internal.v3_4.{expressions => ast}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types.{CTList, CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.ir.api._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.api.util.FreshVariableNamer

import scala.annotation.tailrec

final class PatternConverter()(implicit val irBuilderContext: IRBuilderContext) {

  type Result[A] = State[Pattern[Expr], A]

  def convert(
    p: ast.Pattern,
    knownTypes: Map[ast.Expression, CypherType],
    qualifiedGraphName: QualifiedGraphName,
    pattern: Pattern[Expr] = Pattern.empty): Pattern[Expr] =
    convertPattern(p, knownTypes, qualifiedGraphName).runS(pattern).value

  def convertRelsPattern(
    p: ast.RelationshipsPattern,
    knownTypes: Map[ast.Expression, CypherType],
    qualifiedGraphName: QualifiedGraphName,
    pattern: Pattern[Expr] = Pattern.empty): Pattern[Expr] =
    convertElement(p.element, knownTypes, qualifiedGraphName).runS(pattern).value

  private def convertPattern(p: ast.Pattern, knownTypes: Map[ast.Expression, CypherType], qualifiedGraphName: QualifiedGraphName): Result[Unit] =
    Foldable[List].sequence_[Result, Unit](p.patternParts.toList.map(convertPart(knownTypes, qualifiedGraphName)))

  @tailrec
  private def convertPart(knownTypes: Map[ast.Expression, CypherType], qualifiedGraphName: QualifiedGraphName)(p: ast.PatternPart): Result[Unit] = p match {
    case _: ast.AnonymousPatternPart => stomp(convertElement(p.element, knownTypes, qualifiedGraphName))
    case ast.NamedPatternPart(_, part) => convertPart(knownTypes, qualifiedGraphName)(part)
  }

  private def convertElement(p: ast.PatternElement, knownTypes: Map[ast.Expression, CypherType], qualifiedGraphName: QualifiedGraphName): Result[IRField] =
    p match {

      case np@ast.NodePattern(vOpt, labels: Seq[ast.LabelName], propertiesOpt) =>
        // labels within CREATE patterns, e.g. CREATE (a:Foo), labels for MATCH clauses are rewritten to WHERE
        val patternLabels = labels.map(_.name).toSet

        // labels defined in outside scope, passed in by IRBuilder
        val (knownLabels, qgnOption) = vOpt.flatMap(expr => knownTypes.get(expr)).flatMap {
          case n: CTNode => Some(n.labels -> n.graph)
          case _ => None
        }.getOrElse(Set.empty[String] -> Some(qualifiedGraphName))

        val allLabels = patternLabels ++ knownLabels

        val nodeVar = vOpt match {
          case Some(v) => Var(v.name)(CTNode(allLabels, qgnOption))
          case None => FreshVariableNamer(np.position.offset, CTNode(allLabels, qgnOption))
        }

        for {
          entity <- pure(IRField(nodeVar.name)(nodeVar.cypherType))
          _ <- modify[Pattern[Expr]](_.withEntity(entity, extractProperties(propertiesOpt)))
        } yield entity

      case rc @ast.RelationshipChain(left, ast.RelationshipPattern(eOpt, types, rangeOpt, propertiesOpt, dir, _), right) =>

        val rel = createRelationshipVar(knownTypes, rc.position.offset, eOpt, types, qualifiedGraphName)
        val convertedProperties = extractProperties(propertiesOpt)

        for {
          source <- convertElement(left, knownTypes, qualifiedGraphName)
          target <- convertElement(right, knownTypes, qualifiedGraphName)
          rel <- pure(IRField(rel.name)(if (rangeOpt.isDefined) CTList(rel.cypherType) else rel.cypherType))
          _ <- modify[Pattern[Expr]] { given =>
            val registered = given.withEntity(rel)

            rangeOpt match {
              case Some(Some(range)) =>
                val lower = range.lower.map(_.value.intValue()).getOrElse(1)
                val upper = range.upper
                    .map(_.value.intValue())
                    .getOrElse(throw NotImplementedException("Support for unbounded var-length not yet implemented"))

                Endpoints.apply(source, target) match {
                  case _: IdenticalEndpoints =>
                    throw NotImplementedException("Support for cyclic var-length not yet implemented")

                  case ends: DifferentEndpoints =>
                    dir match {
                      case OUTGOING =>
                        registered.withConnection(rel, DirectedVarLengthRelationship(ends, lower, Some(upper)), convertedProperties)

                      case INCOMING =>
                        registered.withConnection(rel, DirectedVarLengthRelationship(ends.flip, lower, Some(upper)), convertedProperties)

                      case BOTH =>
                        registered.withConnection(rel, UndirectedVarLengthRelationship(ends.flip, lower, Some(upper)), convertedProperties)
                    }
                }

              case None =>
                Endpoints.apply(source, target) match {
                  case ends: IdenticalEndpoints =>
                    registered.withConnection(rel, CyclicRelationship(ends), convertedProperties)

                  case ends: DifferentEndpoints =>
                    dir match {
                      case OUTGOING =>
                        registered.withConnection(rel, DirectedRelationship(ends), convertedProperties)

                      case INCOMING =>
                        registered.withConnection(rel, DirectedRelationship(ends.flip), convertedProperties)

                      case BOTH =>
                        registered.withConnection(rel, UndirectedRelationship(ends), convertedProperties)
                    }
                }

              case _ =>  throw NotImplementedException(s"Support for pattern conversion of $rc not yet implemented")
            }
          }
        } yield target

      case x =>
        throw NotImplementedException(s"Support for pattern conversion of $x not yet implemented")
    }

  private def extractProperties(propertiesOpt: Option[Expression]) = {
    propertiesOpt.map(irBuilderContext.convertExpression) match {
      case Some(e: MapExpression) => Some(e)
      case Some(other) => throw IllegalArgumentException("MapExpression", other)
      case _ => None
    }
  }

  private def createRelationshipVar(
    knownTypes: Map[Expression, CypherType],
    offset: Int,
    eOpt: Option[LogicalVariable],
    types: Seq[RelTypeName],
    qualifiedGraphName: QualifiedGraphName): Var = {

    val patternTypes = types.map(_.name).toSet

    // TODO: Reuse for COPY OF
    // types for ~ / @ are extracted from the referred variable
    //    val equivalence = equivalenceModelOpt.map(convertEquivalenceModel(_, knownTypes, CTRelationship(patternTypes)))
    //    val equivalenceTypes = equivalence.map(_.v.cypherType.asInstanceOf[CTRelationship].types).getOrElse(Set.empty)

    // types defined in outside scope, passed in by IRBuilder
    val (knownRelTypes, qgnOption) = eOpt.flatMap(expr => knownTypes.get(expr)).flatMap {
      case CTRelationship(t, qgn) => Some(t -> qgn)
      case _ => None
    }.getOrElse(Set.empty -> Some(qualifiedGraphName))

    val relTypes = patternTypes ++ knownRelTypes

    val rel = eOpt match {
      case Some(v) => Var(v.name)(CTRelationship(relTypes, qgnOption))
      case None => FreshVariableNamer(offset, CTRelationship(relTypes, qgnOption))
    }
    rel
  }

  private def stomp[T](result: Result[T]): Result[Unit] = result >> pure(())
}
