package org.opencypher.spark.impl.newvalue

import org.opencypher.spark.api.EntityId

class CypherValueConversionTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("NODE conversion") {
    val originalValues = NODE_valueGroups.flatten
    val scalaValues: Seq[(EntityId, NodeData)] = originalValues.map(CypherNode.scalaValue).map(_.orNull)
    val newValues = scalaValues.map {
      case null                             => null
      case ((id: EntityId, data: NodeData)) => CypherNode(id, data.labels, data.properties)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherNode.containsNull(v) should equal(v == null || v.properties.containsNullValue)
    }
  }


  test("MAP conversion") {
    val originalValues = MAP_valueGroups.flatten
    val scalaValues = originalValues.map(CypherMap.scalaValue).map(_.asInstanceOf[Option[Any]]).map(_.orNull)
    val newValues = scalaValues.map {
      case null           => null
      case p: Properties  => CypherMap(p)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherMap.containsNull(v) should equal (v == null || CypherMap.unapply(v).map(_.m).exists(_.values.exists(_ == null)))
    }
  }

  test("LIST conversion") {
    val originalValues = LIST_valueGroups.flatten
    val scalaValues = originalValues.map(CypherList.scalaValue).map(_.orNull)
    val newValues = scalaValues.map {
      case null                 => null
      case l: Seq[CypherValue]  => CypherList(l)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherList.containsNull(v) should equal (v == null || CypherList.unapply(v).exists(_.exists(_ == null)))
    }
  }

  test("STRING conversion") {
    val originalValues = STRING_valueGroups.flatten
    val scalaValues = originalValues.map(CypherString.scalaValue).map(_.orNull)
    val newValues = scalaValues.map {
      case null                => null
      case s: java.lang.String => CypherString(s)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherString.containsNull(v) should equal (v == null)
    }
  }

  test("BOOLEAN conversion") {
    val originalValues = BOOLEAN_valueGroups.flatten
    val scalaValues = originalValues.map(CypherBoolean.scalaValue).map(_.orNull)
    val newValues = scalaValues.map {
      case null                 => null
      case b: java.lang.Boolean => CypherBoolean(b)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherBoolean.containsNull(v) should equal (v == null)
    }
  }

  test("INTEGER conversion") {
    val originalValues = INTEGER_valueGroups.flatten
    val scalaValues = originalValues.map(CypherInteger.scalaValue).map(_.orNull)
    val newValues = scalaValues.map {
      case null              => null
      case l: java.lang.Long => CypherInteger(l)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherInteger.containsNull(v) should equal (v == null)
    }
  }

  test("FLOAT conversion") {
    val originalValues = FLOAT_valueGroups.flatten
    val scalaValues = originalValues.map(CypherFloat.scalaValue).map(_.orNull)
    val newValues = scalaValues.map {
      case null                => null
      case d: java.lang.Double => CypherFloat(d)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherFloat.containsNull(v) should equal (v == null)
    }
  }

  test("NUMBER conversion") {
    val originalValues = NUMBER_valueGroups.flatten
    val scalaValues = originalValues.map(CypherNumber.scalaValue).map(_.orNull)
    val newValues = scalaValues.map {
      case null                => null
      case l: java.lang.Long   => CypherInteger(l)
      case d: java.lang.Double => CypherFloat(d)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherNumber.containsNull(v) should equal (v == null)
    }
  }

  test("ALL conversion") {
    val originalValues = ANY_valueGroups.flatten
    val scalaValues = originalValues.map(CypherValue.scalaValue).map(_.orNull)
    val newValues = scalaValues.map {
      case null => null
      case b: java.lang.Boolean => CypherBoolean(b)
      case s: java.lang.String => CypherString(s)
      case l: java.lang.Long => CypherInteger(l)
      case p: Properties => CypherMap(p)
      case (id: EntityId, data: NodeData) => CypherNode(id, data.labels, data.properties)
      case d: java.lang.Double => CypherFloat(d)
      case l: Seq[_] => CypherList(l.asInstanceOf[Seq[CypherValue]])
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherValue.containsNull(v) should equal (v == null)
    }
  }
}
