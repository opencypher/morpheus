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

  val CATALOG      : P[Unit] = keyword("CATALOG")
  val CREATE       : P[Unit] = keyword("CREATE")
  val LABEL        : P[Unit] = keyword("LABEL")
  val GRAPH        : P[Unit] = keyword("GRAPH")
  val KEY          : P[Unit] = keyword("KEY")
  val WITH         : P[Unit] = keyword("WITH")
  val FROM         : P[Unit] = keyword("FROM")
  val NODE         : P[Unit] = keyword("NODE")
  val NODES        : P[Unit] = keyword("NODES")
  val RELATIONSHIP : P[Unit] = keyword("RELATIONSHIP")
  val SET          : P[Unit] = keyword("SET")
  val SETS         : P[Unit] = keyword("SETS")
  val JOIN         : P[Unit] = keyword("JOIN")
  val ON           : P[Unit] = keyword("ON")
  val AND          : P[Unit] = keyword("AND")
  val AS           : P[Unit] = keyword("AS")
  val SCHEMA       : P[Unit] = keyword("SCHEMA")
  val START        : P[Unit] = keyword("START")
  val END          : P[Unit] = keyword("END")


  // ==== Catalog ====

  // LABEL id ({ foo: STRING, ... } KEY key (foo, ...))
  val labelDefinition: P[LabelDefinition] = {
    val property: P[Property] =
      P(identifier.! ~/ ":" ~/ CypherTypeParser.cypherType)

    val properties: P[Map[String, CypherType]] =
      P("{" ~/ property.rep(min = 1, sep = ",").map(_.toMap) ~/ "}")

    val keyDefinition: P[KeyDefinition] =
      P(KEY ~/ identifier.! ~/ "(" ~/ identifier.!.rep(min = 1, sep = ",").map(_.toSet) ~/ ")")

    P(LABEL ~/ identifier.! ~/ ("(" ~/ properties.? ~/ keyDefinition.? ~/ ")").?).map {
      case (id, None)                          => LabelDefinition(id)
      case (id, Some((maybeProps, maybeKeys))) => maybeProps match {
        case None        => LabelDefinition(id, maybeKeyDefinition = maybeKeys)
        case Some(props) => LabelDefinition(id, props, maybeKeys)
      }
    }
  }

  // [CATALOG] CREATE LABEL <labelDefinition> [KEY <keyDefinition>]
  val catalogLabelDefinition: P[LabelDefinition] =
    P(CATALOG.? ~ CREATE ~ labelDefinition)


  // ==== Schema ====

  val labelCombination: P[LabelCombination] =
    P(identifier.!.rep(min = 1, sep = "," | "&")).map(_.toSet)

  val nodeDefinition: P[NodeDefinition] =
    P("(" ~ labelCombination ~ ")").map(NodeDefinition(_))

  val relType: P[RelationshipType] =
    P(identifier.!)

  val relDefinition: P[RelationshipDefinition] =
    P("[" ~/ relType ~/ "]").map(RelationshipDefinition)

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

  val schemaPatternDefinition: P[SchemaPatternDefinition] = {
    val nodeAlternatives: P[Set[LabelCombination]] =
      P("(" ~ labelCombination.rep(min = 1, sep = "|").map(_.toSet) ~ ")")

    // TODO: Fix symmetric to node
    val relAlternatives: P[Set[String]] =
      P("[" ~/ relType.rep(min = 1, sep = "|") ~/ "]").map(_.toSet)

    P(nodeAlternatives ~ cardinalityConstraint ~/ "-" ~/ relAlternatives ~/ "->" ~/ cardinalityConstraint ~/ nodeAlternatives).map(SchemaPatternDefinition.tupled)
  }

  val localSchemaDefinition: P[SchemaDefinition] =
    // negative lookahead (~ !"-") needed in order to disambiguate node definitions and schema pattern definitions
    P("(" ~/ (labelDefinition | (nodeDefinition ~ !("-" | "<")) | relDefinition | schemaPatternDefinition).rep(sep = ",").map(_.toList) ~/ ")").map(SchemaDefinition)

  val globalSchemaDefinition: P[GlobalSchemaDefinition] =
    P(CREATE ~ GRAPH ~ SCHEMA ~/ identifier.! ~/ localSchemaDefinition).map(GlobalSchemaDefinition.tupled)


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

    P(nodeDefinition ~/ nodeToViewDefinition.rep(min = 1, sep = ",".?).map(_.toList)).map(NodeMappingDefinition.tupled)
  }

  val nodeMappings: P[List[NodeMappingDefinition]] =
    P(nodeMappingDefinition.rep(sep = ",".?).map(_.toList))

  val relationshipMappingDefinition: P[RelationshipMappingDefinition] = {
    val columnIdentifier: P[ColumnIdentifier] =
      P(identifier.!.rep(min = 2, sep = ".").map(_.toList))

    val joinTuple: P[(ColumnIdentifier, ColumnIdentifier)] =
      P(columnIdentifier ~/ "=" ~/ columnIdentifier)

    val joinOnDefinition: P[JoinOnDefinition] =
      P(JOIN ~/ ON ~/ joinTuple.rep(min = 1, sep = AND)).map(_.toList).map(JoinOnDefinition)

    val viewDefinition: P[ViewDefinition] =
      P(viewId ~/ identifier.!).map(ViewDefinition.tupled)

    val labelToViewDefinition: P[LabelToViewDefinition] =
      P(nodeDefinition ~/ FROM ~/ viewDefinition ~/ joinOnDefinition).map(LabelToViewDefinition.tupled)

    val relationshipToViewDefinition: P[RelationshipToViewDefinition] =
      P(FROM ~/ viewDefinition ~/ propertyMappingDefinition.? ~/ START ~/ NODES ~/ labelToViewDefinition ~/ END ~/ NODES ~/ labelToViewDefinition).map(RelationshipToViewDefinition.tupled)

    P(relDefinition ~ relationshipToViewDefinition.rep(min = 1, sep = ",".?).map(_.toList)).map(RelationshipMappingDefinition.tupled)
  }

  val relationshipMappings: P[List[RelationshipMappingDefinition]] =
    P(relationshipMappingDefinition.rep(min = 1, sep = ",".?).map(_.toList))

  val graphDefinition: P[GraphDefinition] = {
    val schemaRefOrDef: P[(Option[String], SchemaDefinition)] =
      P(identifier.! | localSchemaDefinition).map {
        case s: String                          => Some(s) -> SchemaDefinition()
        case schemaDefinition: SchemaDefinition => None -> schemaDefinition
      }

    val graphBody: P[(List[NodeMappingDefinition], List[RelationshipMappingDefinition])] =
      P("(" ~/ nodeMappings.?.map(_.getOrElse(Nil)) ~/ relationshipMappings.?.map(_.getOrElse(Nil)) ~/ ")")

    P(CREATE ~ GRAPH ~ identifier.! ~/ WITH ~/ GRAPH ~/ SCHEMA ~/ schemaRefOrDef ~/ graphBody)
      .map { case (gName, (schemaId, localSchemaDef), (nMappings, rMappings)) => GraphDefinition(gName, schemaId, localSchemaDef, nMappings, rMappings) }
  }

  // ==== DDL ====

  val setSchemaDefinition: P[SetSchemaDefinition] =
    P(SET ~/ SCHEMA ~ identifier.! ~/ "." ~/ identifier.! ~ ";".?).map(SetSchemaDefinition.tupled)

  val ddlStatement: P[DdlStatement] =
    P(setSchemaDefinition | catalogLabelDefinition | globalSchemaDefinition | graphDefinition)

  val ddlDefinitions: P[DdlDefinition] =
    // allow for whitespace/comments at the start
    P(ParsersForNoTrace.noTrace ~ ddlStatement.rep.map(_.toList) ~/ End).map(DdlDefinition)
}
