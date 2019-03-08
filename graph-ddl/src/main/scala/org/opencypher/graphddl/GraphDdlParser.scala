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
package org.opencypher.graphddl

import fastparse._
import fastparse.Parsed.{Failure, Success, TracedFailure}
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.impl.types.CypherTypeParser

case class DdlParsingException(
  index: Int,
  locationPointer: String,
  expected: String,
  tracedFailure: TracedFailure
) extends RuntimeException(
  s"""|Failed at index $index:
      |
      |Expected:\t$expected
      |
      |$locationPointer
      |
      |${tracedFailure.msg}""".stripMargin) with Serializable

object GraphDdlParser {

  def parseDdl(ddlString: String): DdlDefinition = {
    parse(ddlString, ddlDefinitions(_), verboseFailures = true) match {
      case Success(v, _) => v
      case Failure(expected, index, extra) =>
        val before = index - math.max(index - 20, 0)
        val after = math.min(index + 20, extra.input.length) - index
        val locationPointer =
          s"""|\t${extra.input.slice(index - before, index + after).replace('\n', ' ')}
              |\t${"~" * before + "^" + "~" * after}
           """.stripMargin
        throw DdlParsingException(index, locationPointer, expected, extra.trace())
    }
  }

  import org.opencypher.okapi.impl.util.ParserUtils._

  private def CREATE[_: P]: P[Unit] = keyword("CREATE")
  private def ELEMENT[_: P]: P[Unit] = keyword("ELEMENT")
  private def EXTENDS[_: P]: P[Unit] = keyword("EXTENDS")
  private def KEY[_: P]: P[Unit] = keyword("KEY")
  private def GRAPH[_: P]: P[Unit] = keyword("GRAPH")
  private def TYPE[_: P]: P[Unit] = keyword("TYPE")
  private def OF[_: P]: P[Unit] = keyword("OF")
  private def AS[_: P]: P[Unit] = keyword("AS")
  private def FROM[_: P]: P[Unit] = keyword("FROM")
  private def START[_: P]: P[Unit] = keyword("START")
  private def END[_: P]: P[Unit] = keyword("END")
  private def NODES[_: P]: P[Unit] = keyword("NODES")
  private def JOIN[_: P]: P[Unit] = keyword("JOIN")
  private def ON[_: P]: P[Unit] = keyword("ON")
  private def AND[_: P]: P[Unit] = keyword("AND")
  private def SET[_: P]: P[Unit] = keyword("SET")
  private def SCHEMA[_: P]: P[Unit] = keyword("SCHEMA")


  // ==== Element types ====

  private def property[_: P]: P[(String, CypherType)] =
    P(identifier.! ~/ CypherTypeParser.cypherType)

  private def properties[_: P]: P[Map[String, CypherType]] =
    P("(" ~/ property.rep(min = 0, sep = ",").map(_.toMap) ~/ ")")

  private def keyDefinition[_: P]: P[(String, Set[String])] =
    P(KEY ~/ identifier.! ~/ "(" ~/ identifier.!.rep(min = 1, sep = ",").map(_.toSet) ~/ ")")

  private def extendsDefinition[_: P]: P[Set[String]] =
    P(EXTENDS ~/ identifier.!.rep(min = 1, sep = ",").map(_.toSet))

  def elementTypeDefinition[_: P]: P[ElementTypeDefinition] =
    P(identifier.! ~/ extendsDefinition.? ~/ properties.? ~/ keyDefinition.?).map {
      case (id, maybeParents, maybeProps, maybeKey) =>
        ElementTypeDefinition(id, maybeParents.getOrElse(Set.empty), maybeProps.getOrElse(Map.empty), maybeKey)
    }

  def globalElementTypeDefinition[_: P]: P[ElementTypeDefinition] =
    P(CREATE ~ ELEMENT ~/ TYPE ~/ elementTypeDefinition)

  // ==== Schema ====

  def elementType[_: P]: P[String] =
    P(identifier.!)

  def elementTypes[_: P]: P[Set[String]] =
    P(elementType.rep(min = 1, sep = ",")).map(_.toSet)

  def nodeTypeDefinition[_: P]: P[NodeTypeDefinition] =
    P("(" ~ elementTypes ~ ")").map(NodeTypeDefinition(_))

  def relTypeDefinition[_: P]: P[RelationshipTypeDefinition] =
    P(nodeTypeDefinition ~ "-" ~ "[" ~ elementTypes ~ "]" ~ "->" ~ nodeTypeDefinition).map {
      case (startNodeType, eType, endNodeType) => RelationshipTypeDefinition(startNodeType, eType, endNodeType)
    }

  def graphTypeStatements[_: P]: P[List[GraphDdlAst with GraphTypeStatement]] =
    // Note: Order matters here. relTypeDefinition must appear before nodeTypeDefinition since they parse the same prefix
    P("(" ~/ (elementTypeDefinition | relTypeDefinition | nodeTypeDefinition ).rep(sep = "," ~/ Pass).map(_.toList) ~/ ")")

  def graphTypeDefinition[_: P]: P[GraphTypeDefinition] =
    P(CREATE ~ GRAPH ~ TYPE ~/ identifier.! ~/ graphTypeStatements).map(GraphTypeDefinition.tupled)


  // ==== Graph ====

  def viewId[_: P]: P[List[String]] =
    P(escapedIdentifier.repX(min = 1, max = 3, sep = ".")).map(_.toList)

  private def propertyToColumn[_: P]: P[(String, String)] =
    P(identifier.! ~ AS ~/ identifier.!).map { case (column, propertyKey) => propertyKey -> column }

  // TODO: avoid toMap to not accidentally swallow duplicate property keys
  def propertyMappingDefinition[_: P]: P[Map[String, String]] = {
    P("(" ~ propertyToColumn.rep(min = 1, sep = ",").map(_.toMap) ~/ ")")
  }

  def nodeToViewDefinition[_: P]: P[NodeToViewDefinition] =
    P(FROM ~/ viewId ~/ propertyMappingDefinition.?).map(NodeToViewDefinition.tupled)

  def nodeMappingDefinition[_: P]: P[NodeMappingDefinition] = {
    P(nodeTypeDefinition ~ nodeToViewDefinition.rep(min = 1, sep = ",".?).map(_.toList)).map(NodeMappingDefinition.tupled)
  }

  def nodeMappings[_: P]: P[List[NodeMappingDefinition]] =
    P(nodeMappingDefinition.rep(sep = ",").map(_.toList))

  private def columnIdentifier[_: P] =
    P(identifier.!.rep(min = 2, sep = ".").map(_.toList))

  private def joinTuple[_: P]: P[(List[String], List[String])] =
    P(columnIdentifier ~/ "=" ~/ columnIdentifier)

  private def joinOnDefinition[_: P]: P[JoinOnDefinition] =
    P(JOIN ~/ ON ~/ joinTuple.rep(min = 1, sep = AND)).map(_.toList).map(JoinOnDefinition)

  private def viewDefinition[_: P]: P[ViewDefinition] =
    P(viewId ~/ identifier.!).map(ViewDefinition.tupled)

  private def nodeTypeToViewDefinition[_: P]: P[NodeTypeToViewDefinition] =
    P(nodeTypeDefinition ~/ FROM ~/ viewDefinition ~/ joinOnDefinition).map(NodeTypeToViewDefinition.tupled)

  private def relTypeToViewDefinition[_: P]: P[RelationshipTypeToViewDefinition] =
    P(FROM ~/ viewDefinition ~/ propertyMappingDefinition.? ~/ START ~/ NODES ~/ nodeTypeToViewDefinition ~/ END ~/ NODES ~/ nodeTypeToViewDefinition).map(RelationshipTypeToViewDefinition.tupled)

  def relationshipMappingDefinition[_: P]: P[RelationshipMappingDefinition] = {
    P(relTypeDefinition ~ relTypeToViewDefinition.rep(min = 1, sep = ",".?).map(_.toList)).map(RelationshipMappingDefinition.tupled)
  }

  def relationshipMappings[_: P]: P[List[RelationshipMappingDefinition]] =
    P(relationshipMappingDefinition.rep(min = 1, sep = ",").map(_.toList))

  private def graphStatements[_: P]: P[List[GraphDdlAst with GraphStatement]] =
  // Note: Order matters here
    P("(" ~/ (relationshipMappingDefinition | nodeMappingDefinition | elementTypeDefinition | relTypeDefinition | nodeTypeDefinition ).rep(sep = "," ~/ Pass).map(_.toList) ~/ ")")

  def graphDefinition[_: P]: P[GraphDefinition] = {
    P(CREATE ~ GRAPH ~ identifier.! ~/ (OF ~/ identifier.!).? ~/ graphStatements)
      .map { case (gName, graphTypeRef, statements) => GraphDefinition(gName, graphTypeRef, statements) }
  }

  // ==== DDL ====

  def setSchemaDefinition[_: P]: P[SetSchemaDefinition] =
    P(SET ~/ SCHEMA ~ identifier.! ~/ "." ~/ identifier.! ~ ";".?).map(SetSchemaDefinition.tupled)

  def ddlStatement[_: P]: P[GraphDdlAst with DdlStatement] =
    P(setSchemaDefinition | globalElementTypeDefinition | graphTypeDefinition | graphDefinition)

  def ddlDefinitions[_: P]: P[DdlDefinition] =
    // allow for whitespace/comments at the start
    P(Start ~ ddlStatement.rep.map(_.toList) ~/ End).map(DdlDefinition)
}
