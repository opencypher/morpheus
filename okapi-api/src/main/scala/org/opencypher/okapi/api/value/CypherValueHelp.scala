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
