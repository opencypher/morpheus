/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.graphddl

import fastparse.core.Frame
import fastparse.core.Parsed.{Failure, Success}
import org.opencypher.graphddl.GraphDdlAst._
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.types.CypherTypeParser

case class DdlParsingException(
  index: Int,
  locationPointer: String,
  expected: String,
  parserStack: List[Frame]
) extends RuntimeException(
  s"""|Failed at index $index:
      |
      |Expected:\t$expected
      |
      |$locationPointer
      |
      |${parserStack.mkString("\n")}""".stripMargin) with Serializable

object GraphDdlParser {

  def parse(ddlString: String): DdlDefinition = {
    ddlDefinitions.parse(ddlString) match {
      case Success(v, _) => v
      case Failure(failedParser, index, extra) =>
        val before = index - math.max(index - 20, 0)
        val after = math.min(index + 20, extra.input.length) - index
        val locationPointer =
          s"""|\t${extra.input.slice(index - before, index + after).replace('\n', ' ')}
              |\t${"~" * before + "^" + "~" * after}
           """.stripMargin
        throw DdlParsingException(index, locationPointer, extra.traced.expected, extra.traced.stack.toList)
    }
  }

  import fastparse.noApi._
  import org.opencypher.okapi.impl.util.ParserUtils.Whitespace._
  import org.opencypher.okapi.impl.util.ParserUtils._

  private val CREATE       : P[Unit] = keyword("CREATE")
  private val ELEMENT      : P[Unit] = keyword("ELEMENT")
  private val KEY          : P[Unit] = keyword("KEY")
  private val GRAPH        : P[Unit] = keyword("GRAPH")
  private val TYPE         : P[Unit] = keyword("TYPE")
  private val OF           : P[Unit] = keyword("OF")
  private val AS           : P[Unit] = keyword("AS")
  private val FROM         : P[Unit] = keyword("FROM")
  private val START        : P[Unit] = keyword("START")
  private val END          : P[Unit] = keyword("END")
  private val NODES        : P[Unit] = keyword("NODES")
  private val JOIN         : P[Unit] = keyword("JOIN")
  private val ON           : P[Unit] = keyword("ON")
  private val AND          : P[Unit] = keyword("AND")
  private val SET          : P[Unit] = keyword("SET")
  private val SCHEMA       : P[Unit] = keyword("SCHEMA")


  // ==== Element types ====

  val elementTypeDefinition: P[ElementTypeDefinition] = {
    val property: P[Property] =
      P(identifier.! ~/ CypherTypeParser.cypherType)

    val properties: P[Map[String, CypherType]] =
      P("(" ~/ property.rep(min = 1, sep = ",").map(_.toMap) ~/ ")")

    val keyDefinition: P[KeyDefinition] =
      P(KEY ~/ identifier.! ~/ "(" ~/ identifier.!.rep(min = 1, sep = ",").map(_.toSet) ~/ ")")

    P(identifier.! ~/ properties.? ~/ keyDefinition.?).map {
      case (id, None, maybeKey) => ElementTypeDefinition(id, maybeKey = maybeKey)
      case (id, Some(props), maybeKey) => ElementTypeDefinition(id, props, maybeKey)
    }
  }

  val globalElementTypeDefinition: P[ElementTypeDefinition] =
    P(CREATE ~ ELEMENT ~/ TYPE ~/ elementTypeDefinition)


  // ==== Schema ====

  val elementType: P[String] =
    P(identifier.!)

  val elementTypes: P[Set[String]] =
    P(elementType.rep(min = 1, sep = ",")).map(_.toSet)

  val nodeTypeDefinition: P[NodeTypeDefinition] =
    P("(" ~ elementTypes ~ ")").map(NodeTypeDefinition(_))

  val relTypeDefinition: P[RelationshipTypeDefinition] =
    P("[" ~/ elementType ~/ "]").map(RelationshipTypeDefinition)

  val cardinalityConstraint: P[CardinalityConstraint] = {

    val Wildcard: CardinalityConstraint = CardinalityConstraint(0, None)

    val integer: P[Int] =
      P(digit.rep(min = 1).!.map(_.toInt))

    val wildcard: P[Option[Int]] =
      P("*").map(_ => Option.empty[Int])

    val intOrWildcard: P[Option[Int]] =
      P(wildcard | integer.?)

    val fixed: P[CardinalityConstraint] =
      P(intOrWildcard.map(p => CardinalityConstraint(p.getOrElse(0), p)))

    val range: P[CardinalityConstraint] =
      P(integer ~ (".." | ",") ~/ intOrWildcard).map(CardinalityConstraint.tupled)

    P("<" ~/ (range | fixed) ~/ ">").?.map(_.getOrElse(Wildcard))
  }

  val patternDefinition: P[PatternDefinition] = {
    val nodeAlternatives: P[Set[Set[String]]] =
      P("(" ~ elementTypes.rep(min = 1, sep = "|").map(_.toSet) ~ ")")

    // TODO: Fix symmetric to node
    val relAlternatives: P[Set[String]] =
      P("[" ~/ elementType.rep(min = 1, sep = "|") ~/ "]").map(_.toSet)

    P(nodeAlternatives ~ cardinalityConstraint ~ "-" ~/ relAlternatives ~/ "->" ~/ cardinalityConstraint ~/ nodeAlternatives).map(PatternDefinition.tupled)
  }

  val graphTypeStatements: P[List[GraphTypeStatement]] =
    // Note: Order matters here. patternDefinition must appear before nodeTypeDefinition since they parse the same prefix
    P("(" ~/ (elementTypeDefinition | patternDefinition | nodeTypeDefinition | relTypeDefinition).rep(sep = ",").map(_.toList) ~/ ")")

  val graphTypeDefinition: P[GraphTypeDefinition] =
    P(CREATE ~ GRAPH ~ TYPE ~/ identifier.! ~/ graphTypeStatements).map(GraphTypeDefinition.tupled)


  // ==== Graph ====

  val viewId: P[List[String]] =
    P(identifier.!.repX(min = 1, max = 3, sep = ".")).map(_.toList)

  // TODO: avoid toMap to not accidentally swallow duplicate property keys
  val propertyMappingDefinition: P[PropertyToColumnMappingDefinition] = {
    val propertyToColumn: P[(String, String)] =
      P(identifier.! ~ AS ~/ identifier.!).map { case (column, propertyKey) => propertyKey -> column }

    P("(" ~ propertyToColumn.rep(min = 1, sep = ",").map(_.toMap) ~/ ")")
  }

  val nodeMappingDefinition: P[NodeMappingDefinition] = {
    val nodeToViewDefinition: P[NodeToViewDefinition] =
      P(FROM ~/ viewId ~/ propertyMappingDefinition.?).map(NodeToViewDefinition.tupled)

    P(nodeTypeDefinition ~ nodeToViewDefinition.rep(min = 1, sep = ",".?).map(_.toList)).map(NodeMappingDefinition.tupled)
  }

  val nodeMappings: P[List[NodeMappingDefinition]] =
    P(nodeMappingDefinition.rep(sep = ",").map(_.toList))

  val relationshipMappingDefinition: P[RelationshipMappingDefinition] = {
    val columnIdentifier: P[ColumnIdentifier] =
      P(identifier.!.rep(min = 2, sep = ".").map(_.toList))

    val joinTuple: P[(ColumnIdentifier, ColumnIdentifier)] =
      P(columnIdentifier ~/ "=" ~/ columnIdentifier)

    val joinOnDefinition: P[JoinOnDefinition] =
      P(JOIN ~/ ON ~/ joinTuple.rep(min = 1, sep = AND)).map(_.toList).map(JoinOnDefinition)

    val viewDefinition: P[ViewDefinition] =
      P(viewId ~/ identifier.!).map(ViewDefinition.tupled)

    val nodeTypeToViewDefinition: P[NodeTypeToViewDefinition] =
      P(nodeTypeDefinition ~/ FROM ~/ viewDefinition ~/ joinOnDefinition).map(NodeTypeToViewDefinition.tupled)

    val relTypeToViewDefinition: P[RelationshipTypeToViewDefinition] =
      P(FROM ~/ viewDefinition ~/ propertyMappingDefinition.? ~/ START ~/ NODES ~/ nodeTypeToViewDefinition ~/ END ~/ NODES ~/ nodeTypeToViewDefinition).map(RelationshipTypeToViewDefinition.tupled)

    P(relTypeDefinition ~ relTypeToViewDefinition.rep(min = 1, sep = ",".?).map(_.toList)).map(RelationshipMappingDefinition.tupled)
  }

  val relationshipMappings: P[List[RelationshipMappingDefinition]] =
    P(relationshipMappingDefinition.rep(min = 1, sep = ",").map(_.toList))

  val graphDefinition: P[GraphDefinition] = {

    val graphStatements: P[List[GraphStatement]] =
      P("(" ~/ (nodeMappingDefinition | relationshipMappingDefinition | elementTypeDefinition | patternDefinition).rep(sep = ",").map(_.toList) ~/ ")")

    P(CREATE ~ GRAPH ~ identifier.! ~/ (OF ~/ identifier.!).? ~/ graphStatements)
      .map { case (gName, graphTypeRef, statements) => GraphDefinition(gName, graphTypeRef, statements) }
  }

  // ==== DDL ====

  val setSchemaDefinition: P[SetSchemaDefinition] =
    P(SET ~/ SCHEMA ~ identifier.! ~/ "." ~/ identifier.! ~ ";".?).map(SetSchemaDefinition.tupled)

  val ddlStatement: P[DdlStatement] =
    P(setSchemaDefinition | globalElementTypeDefinition | graphTypeDefinition | graphDefinition)

  val ddlDefinitions: P[DdlDefinition] =
    // allow for whitespace/comments at the start
    P(ParsersForNoTrace.noTrace ~ ddlStatement.rep.map(_.toList) ~/ End).map(DdlDefinition)
}
