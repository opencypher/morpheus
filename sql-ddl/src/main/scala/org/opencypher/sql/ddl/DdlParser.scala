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

import fastparse.{WhitespaceApi, core}
import fastparse.core.Parsed.{Failure, Success}
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.sql.ddl.Ddl._

object DdlParser {

  val White = WhitespaceApi.Wrapper {
    import fastparse.all._

    val newline = P("\n" | "\r\n" | "\r" | "\f")
    val whitespace = P(" " | "\t" | newline)
    val comment = P("--" ~ (!newline ~ AnyChar).rep ~ newline)
    NoTrace((comment | whitespace).rep)
  }

  import White._
  import fastparse.noApi._

  val digit = P(CharIn('0' to '9'))
  val character = P(CharIn('a' to 'z', 'A' to 'Z'))
  val identifier = P(character ~ P(character | digit | "_").repX)

  val catalogKeyword = P(IgnoreCase("CATALOG"))
  val createKeyword = P(IgnoreCase("CREATE"))
  val labelKeyword = P(IgnoreCase("LABEL"))
  val graphKeyword = P(IgnoreCase("GRAPH"))
  val schemaKeyword = P(IgnoreCase("SCHEMA"))
  val keyKeyword = P(IgnoreCase("KEY"))
  val withKeyword = P(IgnoreCase("WITH"))
  val fromKeyword = P(IgnoreCase("FROM"))
  val nodeKeyword = P(IgnoreCase("NODE"))
  val setsKeyword = P(IgnoreCase("SETS"))

  val cypherType = P(
    (IgnoreCase("STRING")
      | IgnoreCase("INTEGER")
      | IgnoreCase("FLOAT")
      | IgnoreCase("BOOLEAN")
    ) ~ "?".?)

  val propertyType: P[CypherType] = P(cypherType.!).map { s =>
    CypherType.fromName(s.toUpperCase) match {
      case Some(ct) => ct
      case None => throw IllegalArgumentException("Supported CypherType", s)
    }
  }

  // foo : STRING
  val property: P[Property] = P(identifier.! ~ ":" ~ propertyType)

  // { foo1: STRING, foo2 : BOOLEAN }
  val properties = P("{" ~ property.rep(min = 1, sep = ",").map(_.toMap) ~ "}")

  // ==== Catalog ====

  // A { foo1: STRING, foo2 : BOOLEAN }
  val labelWithProperties: P[EntityDefinition] = P(identifier.! ~ properties.?.map(_.getOrElse(Map.empty[String, CypherType])))

  // LABEL (A { foo1: STRING, foo2 : BOOLEAN }) | LABEL [A { foo1: STRING, foo2 : BOOLEAN }]
  val labelWithoutKeys: P[(String, Map[String, CypherType])] = P(labelKeyword ~ (("(" ~ labelWithProperties ~ ")")
    | ("[" ~ labelWithProperties ~ "]")))

  // KEY A (propKey[, propKey]*))
  val keyDefinition: P[KeyDefinition] = P(keyKeyword ~ identifier.! ~ "(" ~ identifier.!.rep(min = 1, sep = ",").map(_.toSet) ~ ")")

  val localLabelDefinition: P[LabelDefinition] = P(labelWithoutKeys ~ keyDefinition.?).map(LabelDefinition.tupled)

  // [CATALOG] CREATE LABEL <labelDefinition> [KEY <keyDefinition>]
  val catalogLabelDefinition: P[LabelDefinition] = P(catalogKeyword.? ~ createKeyword ~ localLabelDefinition)

  // ==== Schema ====

  // (LabelA [, LabelB]*)
  val nodeDefinition: P[Set[String]] = P("(" ~ identifier.!.rep(min = 1, sep = ",") ~ ")").map(_.toSet)

  // [RelType]
  val relDefinition: P[String] = P("[" ~ identifier.! ~ "]")

  val nodeAlternatives: P[Set[String]] = P("(" ~ identifier.!.rep(min = 1, sep = "|") ~ ")").map(_.toSet)

  val relAlternatives: P[Set[String]] = P("[" ~ identifier.!.rep(min = 1, sep = "|") ~ "]").map(_.toSet)

  val integer: P[Int] = P(digit.rep(min = 1).!.map(_.toInt))

  val wildcard: P[Option[Int]] = P("*").map(_ => Option.empty[Int])

  val intOrWildcard: P[Option[Int]] = P(wildcard | integer.?)

  val fixed: P[CardinalityConstraint] = P(intOrWildcard.map(p => CardinalityConstraint(p.getOrElse(0), p)))

  val Wildcard: CardinalityConstraint = CardinalityConstraint(0, None)

  val range: P[CardinalityConstraint] = P(integer ~ (".." | ",") ~ intOrWildcard).map(CardinalityConstraint.tupled)

  val cardinalityConstraint: P[CardinalityConstraint] = P("<" ~ (range | fixed) ~ ">")

  val schemaPatternDefinition: P[SchemaPatternDefinition] = P(
    nodeAlternatives ~
      cardinalityConstraint.?.map(_.getOrElse(Wildcard)) ~
      "-" ~ relAlternatives ~ "->"
      ~ cardinalityConstraint.?.map(_.getOrElse(Wildcard))
      ~ nodeAlternatives)
    .map(SchemaPatternDefinition.tupled)

  val localSchemaDefinition: P[SchemaDefinition] = P(
    localLabelDefinition.rep(sep = ",".?).map(_.toSet) ~
      nodeDefinition.rep(sep = ",".?).map(_.toSet) ~
      relDefinition.rep(sep = ",".?).map(_.toSet) ~
      schemaPatternDefinition.rep(sep = ",".?).map(_.toSet) ~ ";".?)
    .map(SchemaDefinition.tupled)

  val globalSchemaDefinition: P[(String, SchemaDefinition)] = P(createKeyword ~ graphKeyword ~ schemaKeyword ~ identifier.! ~
    localSchemaDefinition)

  // ==== Graph ====

  val nodeMappingDefinition: P[NodeMappingDefinition] = P(nodeDefinition ~ fromKeyword ~ identifier.!)
    .map(NodeMappingDefinition.tupled)

  // TODO: Add validation that either a global schema or a local schema is defined
  val graphDefinition: P[GraphDefinition] = P(createKeyword ~ graphKeyword ~ identifier.! ~
    withKeyword ~ schemaKeyword ~
    identifier.!.? ~
    ("(" ~ localSchemaDefinition ~ ")").?.map(_.getOrElse(SchemaDefinition())) ~
    nodeKeyword ~ labelKeyword ~ setsKeyword ~ "(" ~ nodeMappingDefinition.rep.map(_.toList) ~ ")"
  ).map(GraphDefinition.tupled)

  //  val relMappingDefinition: P[RelMappingDefinition] = P(relDefinition ~ fromKeyword ~ identifier.!)
  //    .map(NodeMappingDefinition.tupled)


  // ==== DDL ====

  val ddlDefinitions: P[DdlDefinitions] = P(
    catalogLabelDefinition.rep.map(_.toList) ~
      globalSchemaDefinition.rep.map(_.toMap) ~
      graphDefinition.rep.map(_.toList) ~ End
  ).map(DdlDefinitions.tupled)

  def parse(ddlString: String): DdlDefinitions = {
    ddlDefinitions.parse(ddlString) match {
      case Success(v, _) => v
      case Failure(p, index, extra) =>
        val i = extra.input
        val before = index - math.max(index - 20, 0)
        val after = math.min(index + 20, i.length) - index
        println(extra.input.slice(index - before, index + after).replace('\n', ' '))
        println("~" * before + "^" + "~" * after)
        println(s"failed parser: $p at index $index")
        println(s"stack=${extra.traced.stack}")
        // TODO: Throw a helpful parsing error
        throw new Exception("TODO")
    }
  }

  //  val labelDeclarations = "LABELS" ~/ labelDefinition.rep(min = 1, sep = ",").map(_.toList)
  //
  //  val graphDeclaration = P("CREATE" ~/ "GRAPH" ~/ identifier.! ~/ "WITH" ~/ "SCHEMA" ~/
  //    "(" ~/
  //    labelDeclarations.rep.map(_.flatten.toList) ~
  //    entityDeclarations ~
  //    nodeRelationshipNodePattern.rep(sep = ",").map(_.toList) ~
  //    ")"
  //  ).map(GraphDeclaration.tupled)
  //
  //  val nodeToTableMapping = P("NODES" ~ nodeDeclaration.map(_.name) ~ "FROM" ~ identifier.!).map(NodeToTableMapping.tupled)
  //
  //  val mapping = "MAPPING" ~ identifier.! ~ "ONTO" ~ identifier.!
  //
  //  val startNodeMapping = P(mapping ~ "FOR" ~ "START" ~ "NODES" ~/ nodeAlternatives).map {
  //    case (from, to, alternatives) => IdMapping(alternatives, from, to)
  //  }
  //
  //  val endNodeMapping = P(mapping ~ "FOR" ~ "END" ~ "NODES" ~/ nodeAlternatives).map {
  //    case (from, to, alternatives) => IdMapping(alternatives, from, to)
  //  }
  //
  //  val relToTableMapping = P("RELATIONSHIPS" ~/ relDeclaration.map(_.name) ~/ "FROM" ~/ identifier.! ~/
  //    startNodeMapping.rep(min = 1).map(_.toList) ~
  //    endNodeMapping.rep(min = 1).map(_.toList)
  //  ).map(RelationshipToTableMapping.tupled).log()
  //
  //  val labelsForTablesMapping = {
  //    P(nodeToTableMapping.rep.map(_.toList) ~
  //      relToTableMapping.rep.map(_.toList)
  //    ).map(LabelsForTablesMapping.tupled)
  //  }
  //

}
