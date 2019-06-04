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
package org.opencypher.okapi.ir.impl

import cats._
import cats.data.State
import cats.data.State._
import cats.instances.list._
import cats.syntax.flatMap._
import org.opencypher.okapi.api.graph.{Pattern => _, _}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, NotImplementedException}
import org.opencypher.okapi.impl.types.CypherTypeUtils._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.api.util.FreshVariableNamer
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.opencypher.v9_0.expressions.{Expression, LogicalVariable, RelTypeName}
import org.opencypher.v9_0.{expressions => ast}

import scala.annotation.tailrec

final class PatternConverter(irBuilderContext: IRBuilderContext) {

  type Result[A] = State[Pattern, A]

  def convert(
    p: ast.Pattern,
    knownTypes: Map[ast.Expression, CypherType],
    qualifiedGraphName: QualifiedGraphName,
    pattern: Pattern = Pattern.empty
  ): Pattern =
    convertPattern(p, knownTypes, qualifiedGraphName).runS(pattern).value

  def convertRelsPattern(
    p: ast.RelationshipsPattern,
    knownTypes: Map[ast.Expression, CypherType],
    qualifiedGraphName: QualifiedGraphName,
    pattern: Pattern = Pattern.empty
  ): Pattern =
    convertElement(p.element, knownTypes, qualifiedGraphName).runS(pattern).value

  private def convertPattern(
    p: ast.Pattern,
    knownTypes: Map[ast.Expression, CypherType],
    qualifiedGraphName: QualifiedGraphName
  ): Result[Unit] =
    Foldable[List].sequence_[Result, Unit](p.patternParts.toList.map(convertPart(knownTypes, qualifiedGraphName)))

  @tailrec
  private def convertPart(knownTypes: Map[ast.Expression, CypherType], qualifiedGraphName: QualifiedGraphName)
    (p: ast.PatternPart): Result[Unit] = p match {
    case _: ast.AnonymousPatternPart => stomp(convertElement(p.element, knownTypes, qualifiedGraphName))
    case ast.NamedPatternPart(_, part) => convertPart(knownTypes, qualifiedGraphName)(part)
  }

  private def convertElement(
    p: ast.PatternElement,
    knownTypes: Map[ast.Expression, CypherType],
    qualifiedGraphName: QualifiedGraphName
  ): Result[PatternElement] =
    p match {

      case np@ast.NodePattern(vOpt, labels: Seq[ast.LabelName], propertiesOpt, baseNodeVar) =>
        // labels within CREATE patterns, e.g. CREATE (a:Foo), labels for MATCH clauses are rewritten to WHERE
        val patternLabels = labels.map(_.name).toSet

        val baseNodeCypherTypeOpt = baseNodeVar.map(knownTypes)
        val baseNodeLabels = baseNodeCypherTypeOpt.map(_.toCTNode.labels).getOrElse(Set.empty)

        // labels defined in outside scope, passed in by IRBuilder
        val (knownLabels, qgnOption) = vOpt.flatMap(expr => knownTypes.get(expr)).flatMap {
          case n: CTNode => Some(n.labels -> n.graph)
          case _ => None
        }.getOrElse(Set.empty[String] -> Some(qualifiedGraphName))

        val allLabels = patternLabels ++ knownLabels ++ baseNodeLabels

        val elementName = vOpt.map(_.name).getOrElse(FreshVariableNamer(np.position.offset, CTNode).name)

        val maybeBaseNodeElement = baseNodeVar.map { x =>
          val labels = knownTypes(x) match {
            case CTNode(labels, _ ) => labels
            case _ => ???
          }
          NodeElement(x.name, labels)
        }

        for {
          element <- pure(NodeElement(elementName, allLabels))
          _ <- modify[Pattern](_.withElement(element, extractProperties(propertiesOpt)).withBaseElement(element, maybeBaseNodeElement))
        } yield element

      case rc@ast.RelationshipChain(left, ast.RelationshipPattern(eOpt, types, rangeOpt, propertiesOpt, direction, _, baseRelVar), right) =>

        val relElement = createRelationshipElement(knownTypes, rc.position.offset, eOpt, types, baseRelVar, qualifiedGraphName)
        val convertedProperties = extractProperties(propertiesOpt)

        val maybeBaseRelElement = baseRelVar.map { x =>
          val relTypes = knownTypes(x) match {
            case CTRelationship(types, _ ) => types
            case _ => ???
          }
          RelationshipElement(x.name, relTypes)
        }

        for {
          source <- convertElement(left, knownTypes, qualifiedGraphName)
          target <- convertElement(right, knownTypes, qualifiedGraphName)
          _ <- modify[Pattern] { given =>
            val registered = given
              .withElement(relElement, convertedProperties)
              .withBaseElement(relElement, maybeBaseRelElement)

            val bounds = rangeOpt match {
              case Some(Some(range)) =>
                val lower = range.lower.map(_.value.intValue()).getOrElse(1)
                val upper = range.upper
                  .map(_.value.intValue())
                  .getOrElse(throw NotImplementedException("Support for unbounded var-length not yet implemented"))

                lower -> upper
              case Some(None) => throw NotImplementedException("Support for unbounded var-length not yet implemented")
              case None => 1 -> 1
            }

            val (src, tgt, dir) = direction match {
              case OUTGOING => (source, target, Outgoing)
              case INCOMING => (target, source, Incoming)
              case BOTH => (source, target, Both)
            }

            val connection = Connection(Some(src), Some(tgt), dir, bounds._1, bounds._2)

            registered.withConnection(relElement, connection)
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

  private def createRelationshipElement(
    knownTypes: Map[Expression, CypherType],
    offset: Int,
    eOpt: Option[LogicalVariable],
    types: Seq[RelTypeName],
    baseRelOpt: Option[LogicalVariable],
    qualifiedGraphName: QualifiedGraphName
  ): RelationshipElement = {

    val patternTypes = types.map(_.name).toSet

    val baseRelCypherTypeOpt = baseRelOpt.map(knownTypes)
    val baseRelTypes = baseRelCypherTypeOpt.map(_.toCTRelationship.types).getOrElse(Set.empty)

    // types defined in outside scope, passed in by IRBuilder
    val (knownRelTypes, qgnOption) = eOpt.flatMap(expr => knownTypes.get(expr)).flatMap {
      case CTRelationship(t, qgn) => Some(t -> qgn)
      case _ => None
    }.getOrElse(Set.empty[String] -> Some(qualifiedGraphName))

    val relTypes = {
      if (patternTypes.nonEmpty) patternTypes
      else if (baseRelTypes.nonEmpty) baseRelTypes
      else knownRelTypes
    }

    val relName = eOpt.map(_.name).getOrElse(FreshVariableNamer(offset, CTRelationship).name)

    RelationshipElement(relName , relTypes)
  }

  private def stomp[T](result: Result[T]): Result[Unit] = result >> pure(())
}
