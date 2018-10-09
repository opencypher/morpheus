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
import fastparse.core.Parsed.{Failure, Success}
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.impl.exception.IllegalArgumentException

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

  val cypherType = P(
    (IgnoreCase("STRING")
      | IgnoreCase("INTEGER")
      | IgnoreCase("FLOAT")
      | IgnoreCase("BOOLEAN")
    ) ~ "?".?)

  val propertyType: P[CypherType] = P(cypherType.!).map { s =>
    CypherType.fromName(s) match {
      case Some(ct) => ct
      case None => throw IllegalArgumentException("Supported CypherType", s)
    }
  }

  val property: P[Property] = {
    val propertyName = identifier.!
    val propertyType = identifier.!.map(PropertyType.apply)
    P(propertyName ~ propertyType).map(Property.tupled)
  }


  object PropertyType {
    def apply(name: String): PropertyType = {
      name match {
        case "STRING" => StringType
        case "INTEGER" => IntegerType
        case other => throw new UnsupportedOperationException(s"Property type $other")
      }
    }
  }

  val labelDeclaration = {
    val labelName = identifier.!
    val properties = "PROPERTIES" ~/ "(" ~/ property.rep(sep = ",").map(_.toList) ~/ ")"
    P(labelName ~ properties.?.map(_.getOrElse(List.empty))).map(LabelDeclaration.tupled)
  }

  val nodeDeclaration = ("(" ~ identifier.! ~ ")").map(NodeDeclaration)

  val nodeAlternatives = ("(" ~ identifier.!.rep(min = 1, sep = "|") ~ ")").map(_.toSet)

  val relDeclaration = ("[" ~ identifier.! ~ "]").map(RelDeclaration)

  val relAlternatives = ("[" ~ identifier.!.rep(min = 1, sep = "|") ~ "]").map(_.toSet)

  val entityDeclarations = (nodeDeclaration | relDeclaration).rep(sep = ",").map(_.toList)

  val integer = digit.rep(min = 1).!.map(_.toInt)

  val wildcard = "*".!.map(_ => Option.empty[Int])

  val intOrWildcard = integer.? | wildcard

  val fixed = intOrWildcard.map(p => CardinalityConstraint(p, p))

  val Wildcard = CardinalityConstraint(None, None)

  val range = (integer.? ~ (".." | ",") ~ intOrWildcard).map(CardinalityConstraint.tupled)

  val cardinalityConstraint: P[CardinalityConstraint] = ("<" ~ (fixed | range) ~ ">").?.map(_.getOrElse(Wildcard))

  val nodeRelationshipNodePattern = P(
    nodeAlternatives ~ cardinalityConstraint ~
      "-" ~ relAlternatives ~ "->"
      ~ cardinalityConstraint ~ nodeAlternatives)
    .map(BasicPattern.tupled)

  val labelDeclarations = "LABELS" ~/ labelDeclaration.rep(min = 1, sep = ",").map(_.toList)

  val graphDeclaration = P("CREATE" ~/ "GRAPH" ~/ identifier.! ~/ "WITH" ~/ "SCHEMA" ~/
    "(" ~/
    labelDeclarations.rep.map(_.flatten.toList) ~
    entityDeclarations ~
    nodeRelationshipNodePattern.rep(sep = ",").map(_.toList) ~
    ")"
  ).map(GraphDeclaration.tupled)

  val nodeToTableMapping = P("NODES" ~ nodeDeclaration.map(_.name) ~ "FROM" ~ identifier.!).map(NodeToTableMapping.tupled)

  val mapping = "MAPPING" ~ identifier.! ~ "ONTO" ~ identifier.!

  val startNodeMapping = P(mapping ~ "FOR" ~ "START" ~ "NODES" ~/ nodeAlternatives).map {
    case (from, to, alternatives) => IdMapping(alternatives, from, to)
  }

  val endNodeMapping = P(mapping ~ "FOR" ~ "END" ~ "NODES" ~/ nodeAlternatives).map {
    case (from, to, alternatives) => IdMapping(alternatives, from, to)
  }

  val relToTableMapping = P("RELATIONSHIPS" ~/ relDeclaration.map(_.name) ~/ "FROM" ~/ identifier.! ~/
    startNodeMapping.rep(min = 1).map(_.toList) ~
    endNodeMapping.rep(min = 1).map(_.toList)
  ).map(RelationshipToTableMapping.tupled).log()

  val labelsForTablesMapping = {
    P(nodeToTableMapping.rep.map(_.toList) ~
      relToTableMapping.rep.map(_.toList)
    ).map(LabelsForTablesMapping.tupled)
  }

  val ddl = P(graphDeclaration.rep(min = 1).map(_.toList) ~ labelsForTablesMapping).map(Ddl.tupled)

  def parse(ddlString: String): Ddl = {
    ddl.parse(ddlString) match {
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

}
