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
package org.opencypher.okapi.api.value

import org.opencypher.okapi.api.value.CypherValue.Element.{idJsonKey, propertiesJsonKey}
import org.opencypher.okapi.api.value.CypherValue.Node.labelsJsonKey
import org.opencypher.okapi.api.value.CypherValue.{CypherBigDecimal, CypherBoolean, CypherFloat, CypherInteger, CypherList, CypherMap, CypherNull, CypherString, CypherValue, Node, Relationship}
import org.opencypher.okapi.api.value.CypherValue.Relationship.{endIdJsonKey, startIdJsonKey, typeJsonKey}
import ujson.{Bool, Null, Num, Obj, Str, Value}

object CypherValueHelp {

  def toJson(v: CypherValue)(implicit formatValue: Any => String): Value = {
    v match {
      case CypherNull => Null
      case CypherString(s) => Str(s)
      case CypherList(l) => l.map(toJson)
      case CypherMap(m) => m.mapValues(toJson).toSeq.sortBy(_._1)
      case Relationship(id, startId, endId, relType, properties) =>
        Obj(
          idJsonKey -> Str(formatValue(id)),
          typeJsonKey -> Str(relType),
          startIdJsonKey -> Str(formatValue(startId)),
          endIdJsonKey -> Str(formatValue(endId)),
          propertiesJsonKey -> toJson(properties)
        )
      case Node(id, labels, properties) =>
        Obj(
          idJsonKey -> Str(formatValue(id)),
          labelsJsonKey -> labels.toSeq.sorted.map(Str),
          propertiesJsonKey -> toJson(properties)
        )
      case CypherFloat(d) => Num(d)
      case CypherInteger(l) => Str(l.toString) // `Num` would lose precision
      case CypherBoolean(b) => Bool(b)
      case CypherBigDecimal(b) => Obj(
        "type" -> Str("BigDecimal"),
        "scale" -> Num(b.bigDecimal.scale()),
        "precision" -> Num(b.bigDecimal.precision())
      )
      case other => Str(formatValue(other.value))
    }
  }

}
