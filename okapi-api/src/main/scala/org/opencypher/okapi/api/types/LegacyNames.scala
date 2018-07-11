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
package org.opencypher.okapi.api.types

import org.opencypher.okapi.api.types.CypherType._

object LegacyNames {

  implicit class TypeWithLegacyName(val ct: CypherType) extends AnyVal {
    def legacyName: String = {
      val nullableSuffix = if (ct.isNullable) "?" else ""
      ct match {
        case CTVoid => "VOID"
        case CTNull => "NULL"
        case _ if ct.subTypeOf(CTNoLabel.nullable) =>
          s"NODE()$nullableSuffix"
        case CTNode(labels) =>
          if (labels.isEmpty) {
            s"NODE$nullableSuffix"
          } else {
            s"NODE(${labels.mkString(":", ":", "")})$nullableSuffix"
          }
        case CTRelationship(relTypes) =>
          if (relTypes.isEmpty) {
            s"RELATIONSHIP$nullableSuffix"
          } else {
            s"RELATIONSHIP(${relTypes.mkString(":", "|", "")})$nullableSuffix"
          }
        case CTList(elementType) => s"LIST OF ${elementType.legacyName}"
        case _ if ct.subTypeOf(CTAnyMap.nullable) => s"MAP$nullableSuffix"
        case u: CTUnion =>
          import u._
          if (u == CTNumber || u == CTNumber.nullable) {
            s"NUMBER$nullableSuffix"
          } else if (subTypeOf(CTAnyList.nullable)) {
            val elementType = ors.collect { case CTList(et) => et }.head
            s"LIST$nullableSuffix OF ${elementType.legacyName}"
          } else if (isNullable && ors.size == 2) {
            s"${material.legacyName}$nullableSuffix"
          } else if (!isNullable && ors.size == 1) {
            ors.head.legacyName
          } else {
            // Non-legacy type
            name
          }
        case _ => ct.name
      }
    }
  }

  /**
    * Parses the name of CypherType into the actual CypherType object.
    *
    * @param name string representation of the CypherType
    * @return
    * @see {{{org.opencypher.okapi.api.types.CypherType#name}}}
    */
  def fromLegacyName(name: String): Option[CypherType] = {
    def extractLabels(s: String, typ: String, sep: String): Option[Set[String]] = {
      val regex = s"""$typ\\(:(.+)\\).?""".r
      s match {
        case regex(l) => Some(l.split(sep).toSet)
        case _ => None
      }
    }

    val noneNullType: Option[CypherType] = name match {
      case "STRING" | "STRING?" => Some(CTString)
      case "INTEGER" | "INTEGER?" => Some(CTInteger)
      case "FLOAT" | "FLOAT?" => Some(CTFloat)
      case "NUMBER" | "NUMBER?" => Some(CTNumber)
      case "BOOLEAN" | "BOOLEAN?" => Some(CTBoolean)
      case "ANY" | "ANY?" => Some(CTAny)
      case "VOID" | "VOID?" => Some(CTVoid)
      case "NULL" | "NULL?" => Some(CTNull)
      case "MAP" | "MAP?" => Some(CTAnyMap)
      case "PATH" | "PATH?" => Some(CTPath)
      case "?" | "??" => Some(CTAny)

      case node if node.startsWith("NODE") =>
        extractLabels(node, "NODE", ":") match {
          case None => Some(CTAnyNode)
          case Some(ls) if ls.isEmpty => Some(CTNoLabel)
          case Some(ls) => Some(CTNode(ls))
        }

      case rel if rel.startsWith("RELATIONSHIP") =>
        extractLabels(rel, "RELATIONSHIP", """\|""") match {
          case None => Some(CTAnyRelationship)
          case Some(rts) if rts.isEmpty => None
          case Some(rts) => Some(CTRelationship(rts))
        }

      case list if list.startsWith("LIST") =>
        fromLegacyName(list.replaceFirst("""LIST\?? OF """, ""))
          .map(CTList)

      case _ => None
    }

    noneNullType.map(ct => if (name == ct.legacyName) ct else ct.nullable)
  }

}
