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
package org.opencypher.sql.ddl

import fastparse.WhitespaceApi
import fastparse.core.Frame
import fastparse.core.Parsed.{Failure, Success}
import org.opencypher.okapi.api.types._
import org.opencypher.sql.ddl.Ddl._

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

object DdlParser {

  def parse(ddlString: String): DdlDefinitions = {
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

  object ParsersForNoTrace {

    import fastparse.all._

    val newline: P[Unit] = P("\n" | "\r\n" | "\r" | "\f")
    val whitespace: P[Unit] = P(" " | "\t" | newline)
    val comment: P[Unit] = P("--" ~ (!newline ~ AnyChar).rep ~ newline)
    val noTrace: P[Unit] = (comment | whitespace).rep
  }

  val Whitespace = WhitespaceApi.Wrapper {
    import fastparse.all._
    NoTrace(ParsersForNoTrace.noTrace)
  }

  import Whitespace._
  import fastparse.noApi._

  implicit class RichParser[T](parser: fastparse.core.Parser[T, Char, String]) {
    def entireInput: P[T] = parser ~ End
  }

  val digit: P[Unit] = P(CharIn('0' to '9'))
  val character: P[Unit] = P(CharIn('a' to 'z', 'A' to 'Z'))
  val identifier: P[Unit] = P(character ~~ P(character | digit | "_").repX)

  def keyword(k: String): P[Unit] = P(IgnoreCase(k))

  val CATALOG: P[Unit] = keyword("CATALOG")
  val CREATE: P[Unit] = keyword("CREATE")
  val LABEL: P[Unit] = keyword("LABEL")
  val GRAPH: P[Unit] = keyword("GRAPH")
  val KEY: P[Unit] = keyword("KEY")
  val WITH: P[Unit] = keyword("WITH")
  val FROM: P[Unit] = keyword("FROM")
  val NODE: P[Unit] = keyword("NODE")
  val NODES: P[Unit] = keyword("NODES")
  val RELATIONSHIP: P[Unit] = keyword("RELATIONSHIP")
  val SET: P[Unit] = keyword("SET")
  val SETS: P[Unit] = keyword("SETS")
  val JOIN: P[Unit] = keyword("JOIN")
  val ON: P[Unit] = keyword("ON")
  val AND: P[Unit] = keyword("AND")
  val AS: P[Unit] = keyword("AS")
  val SCHEMA: P[Unit] = keyword("SCHEMA")
  val START: P[Unit] = keyword("START")
  val END: P[Unit] = keyword("END")

  val propertyType: P[CypherType] = P(
    (IgnoreCase("STRING").!.map(_ => CTString)
      | IgnoreCase("INTEGER").!.map(_ => CTInteger)
      | IgnoreCase("FLOAT").!.map(_ => CTFloat)
      | IgnoreCase("BOOLEAN").!.map(_ => CTBoolean)
    ) ~ "?".!.?.map(_.isDefined)).map { case (cypherType, isNullable) =>
    if (isNullable) cypherType.nullable else cypherType
  }

  // foo : STRING
  val property: P[Property] = P(identifier.! ~/ ":" ~/ propertyType)

  // { foo1: STRING, foo2 : BOOLEAN }
  val properties: P[Map[String, CypherType]] = P("{" ~/ property.rep(min = 1, sep = ",").map(_.toMap) ~/ "}")

  // ==== Catalog ====

  // KEY A (propKey[, propKey]*))
  val keyDefinition: P[KeyDefinition] = P(KEY ~/ identifier.! ~/ "(" ~/ identifier.!.rep(min = 1, sep = ",").map(_.toSet) ~/ ")")

  val innerLabelDefinition: P[LabelDefinition] = P(identifier.! ~/ properties.?.map(_.getOrElse(Map.empty[String, CypherType])) ~/ keyDefinition.?).map(LabelDefinition.tupled)

  val labelDefinition: P[LabelDefinition] = P(LABEL ~/ (("(" ~/ innerLabelDefinition ~/ ")") | ("[" ~/ innerLabelDefinition ~/ "]")))

  // [CATALOG] CREATE LABEL <labelDefinition> [KEY <keyDefinition>]
  val catalogLabelDefinition: P[LabelDefinition] = P(CATALOG.? ~ CREATE ~ labelDefinition)

  // ==== Schema ====

  val labelCombination: P[LabelCombination] = P(identifier.!.rep(min = 1, sep = "," | "&")).map(_.toSet)

  val nodeDefinition: P[LabelCombination] = P("(" ~ labelCombination ~ ")")

  val relType: P[String] = P(identifier.!)

  val relDefinition: P[String] = P("[" ~/ relType ~/ "]")

  val nodeAlternatives: P[Set[LabelCombination]] = P("(" ~ labelCombination.rep(min = 1, sep = "|").map(_.toSet) ~ ")")

  // TODO: Fix symmetric to node
  val relAlternatives: P[Set[String]] = P("[" ~/ relType.rep(min = 1, sep = "|") ~/ "]").map(_.toSet)

  val integer: P[Int] = P(digit.rep(min = 1).!.map(_.toInt))

  val wildcard: P[Option[Int]] = P("*").map(_ => Option.empty[Int])

  val intOrWildcard: P[Option[Int]] = P(wildcard | integer.?)

  val fixed: P[CardinalityConstraint] = P(intOrWildcard.map(p => CardinalityConstraint(p.getOrElse(0), p)))

  val Wildcard: CardinalityConstraint = CardinalityConstraint(0, None)

  val range: P[CardinalityConstraint] = P(integer ~ (".." | ",") ~/ intOrWildcard).map(CardinalityConstraint.tupled)

  val cardinalityConstraint: P[CardinalityConstraint] = P("<" ~/ (range | fixed) ~/ ">")

  val schemaPatternDefinition: P[SchemaPatternDefinition] = P(
    nodeAlternatives ~
      cardinalityConstraint.?.map(_.getOrElse(Wildcard)) ~/
      "-" ~/ relAlternatives ~/ "->"
      ~/ cardinalityConstraint.?.map(_.getOrElse(Wildcard))
      ~/ nodeAlternatives)
    .map(SchemaPatternDefinition.tupled)

  val localSchemaDefinition: P[SchemaDefinition] = P(
    labelDefinition.rep(sep = ",".?).map(_.toSet) ~/ ",".? ~/
      // negative lookahead (~ !"-") needed in order to disambiguate node definitions and schema pattern definitions
      (nodeDefinition ~ !"-").rep(sep = ",".?).map(_.toSet) ~ ",".? ~/
      relDefinition.rep(sep = ",".?).map(_.toSet) ~/ ",".? ~/
      schemaPatternDefinition.rep(sep = ",".?).map(_.toSet) ~/ ";".?)
    .map(SchemaDefinition.tupled)

  val globalSchemaDefinition: P[(String, SchemaDefinition)] = P(CREATE ~ GRAPH ~ SCHEMA ~/ identifier.! ~/ localSchemaDefinition)

  // ==== Graph ====

  val propertyToColumn: P[(String, String)] = P(identifier.! ~ AS ~/ identifier.!).map { case (column, propertyKey) => propertyKey -> column }
  val propertyMappingDefinition: P[PropertyToColumnMappingDefinition] = P("(" ~ propertyToColumn.rep(min = 1, sep = ",").map(_.toMap) ~/ ")")

  val nodeMappingDefinition: P[NodeMappingDefinition] = P(nodeDefinition ~/ FROM ~/ identifier.! ~/ propertyMappingDefinition.?).map(NodeMappingDefinition.tupled)
  val nodeMappings: P[List[NodeMappingDefinition]] = P(NODE ~/ LABEL ~/ SETS ~/ "(" ~/ nodeMappingDefinition.rep(sep = ",".?).map(_.toList) ~/ ")")

  val columnIdentifier: P[ColumnIdentifier] = P(identifier.!.rep(min = 2, sep = ".").map(_.toList))
  val joinTuple: P[(ColumnIdentifier, ColumnIdentifier)] = P(columnIdentifier ~/ "=" ~/ columnIdentifier)
  val joinOnDefinition: P[JoinOnDefinition] = P(JOIN ~/ ON ~/ joinTuple.rep(min = 1, sep = AND)).map(_.toList).map(JoinOnDefinition)

  val sourceViewDefinition: P[SourceViewDefinition] = P(identifier.! ~/ identifier.!).map(SourceViewDefinition.tupled)

  val labelToViewDefinition: P[LabelToViewDefinition] = P(LABEL ~/ SET ~/ nodeDefinition ~/ FROM ~/ sourceViewDefinition ~/ joinOnDefinition).map(LabelToViewDefinition.tupled)

  val relationshipToViewDefinition: P[RelationshipToViewDefinition] = P(FROM ~/ sourceViewDefinition ~/ START ~/ NODES ~/ labelToViewDefinition ~/ END ~/ NODES ~/ labelToViewDefinition).map(RelationshipToViewDefinition.tupled)
  val relationshipMappingDefinition: P[RelationshipMappingDefinition] = P("(" ~ relType ~ ")" ~ relationshipToViewDefinition.rep(min = 1, sep = ",".?).map(_.toList)).map(RelationshipMappingDefinition.tupled)
  val relationshipMappings: P[List[RelationshipMappingDefinition]] = P(RELATIONSHIP ~/ LABEL ~/ SETS ~/ "(" ~ relationshipMappingDefinition.rep(min = 1, sep = ",".?).map(_.toList) ~/ ")")

  // TODO: this allows WITH SCHEMA with missing identifier and missing inline schema -> forbid
  val graphDefinition: P[GraphDefinition] = P(CREATE ~ GRAPH ~ identifier.! ~/ WITH ~/ GRAPH ~/ SCHEMA ~/ identifier.!.? ~/
    ("(" ~/ localSchemaDefinition ~/ ")").?.map(_.getOrElse(SchemaDefinition())) ~/
    nodeMappings.?.map(_.getOrElse(Nil)) ~/
    relationshipMappings.?.map(_.getOrElse(Nil))
  ).map(GraphDefinition.tupled)

  // ==== DDL ====

  val setSchemaDefinition: P[SetSchemaDefinition] = P(SET ~/ SCHEMA ~ identifier.! ~/ "." ~/ identifier.!.? ~ ";".?).map(SetSchemaDefinition.tupled)

  val ddlDefinitions: P[DdlDefinitions] = P(
    ParsersForNoTrace.noTrace ~ // allow for whitespace/comments at the start
      setSchemaDefinition.? ~/
      catalogLabelDefinition.rep.map(_.toList) ~/
      globalSchemaDefinition.rep.map(_.toMap) ~/
      graphDefinition.rep.map(_.toList) ~/ End
  ).map(DdlDefinitions.tupled)

}
