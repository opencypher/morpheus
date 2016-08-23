package org.opencypher.spark.api.value

class CypherValueConversionTest extends CypherValueTestSuite {

  import CypherTestValues._

  test("PATH conversion") {
    val originalValues = PATH_valueGroups.flatten
    val scalaValues = originalValues.map(CypherPath.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null             => null
      case elements: Seq[_] => CypherPath(elements.asInstanceOf[Seq[CypherEntityValue]]: _*)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherPath.isComparable(v) should equal(v == null)
    }
  }

  test("RELATIONSHIP conversion") {
    val originalValues = RELATIONSHIP_valueGroups.flatten
    val scalaValues: Seq[(EntityId, RelationshipData)] = originalValues.map(CypherRelationship.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null                             => null
      case ((id: EntityId, data: RelationshipData)) => CypherRelationship(id, data.startId, data.endId, data.relationshipType, data.properties)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherRelationship.isComparable(v) should equal(v == null)
    }
  }

  test("NODE conversion") {
    val originalValues = NODE_valueGroups.flatten
    val scalaValues: Seq[(EntityId, NodeData)] = originalValues.map(CypherNode.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null                             => null
      case ((id: EntityId, data: NodeData)) => CypherNode(id, data.labels, data.properties)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherNode.isComparable(v) should equal(v == null)
    }
  }

  test("MAP conversion") {
    val originalValues = MAP_valueGroups.flatten
    val scalaValues = originalValues.map(CypherMap.contents).map(_.asInstanceOf[Option[Any]]).map(_.orNull)
    val newValues = scalaValues.map {
      case null           => null
      case p: Properties  => CypherMap(p)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherMap.isComparable(v) should equal (v == null || CypherMap.unapply(v).map(_.m).exists(_.values.exists(_ == null)))
    }
  }

  test("LIST conversion") {
    val originalValues = LIST_valueGroups.flatten
    val scalaValues = originalValues.map(CypherList.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null                 => null
      case l: Seq[CypherValue]  => CypherList(l)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherList.isComparable(v) should equal (v == null || CypherList.unapply(v).exists(_.exists(_ == null)))
    }
  }

  test("STRING conversion") {
    val originalValues = STRING_valueGroups.flatten
    val scalaValues = originalValues.map(CypherString.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null                => null
      case s: java.lang.String => CypherString(s)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherString.isComparable(v) should equal (v == null)
    }
  }

  test("BOOLEAN conversion") {
    val originalValues = BOOLEAN_valueGroups.flatten
    val scalaValues = originalValues.map(CypherBoolean.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null                 => null
      case b: java.lang.Boolean => CypherBoolean(b)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherBoolean.isComparable(v) should equal (v == null)
    }
  }

  test("INTEGER conversion") {
    val originalValues = INTEGER_valueGroups.flatten
    val scalaValues = originalValues.map(CypherInteger.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null              => null
      case l: java.lang.Long => CypherInteger(l)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherInteger.isComparable(v) should equal (v == null)
    }
  }

  test("FLOAT conversion") {
    val originalValues = FLOAT_valueGroups.flatten
    val scalaValues = originalValues.map(CypherFloat.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null                => null
      case d: java.lang.Double => CypherFloat(d)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherFloat.isComparable(v) should equal (v == null)
    }
  }

  test("NUMBER conversion") {
    val originalValues = NUMBER_valueGroups.flatten
    val scalaValues = originalValues.map(CypherNumber.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null                => null
      case l: java.lang.Long   => CypherInteger(l)
      case d: java.lang.Double => CypherFloat(d)
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherNumber.isComparable(v) should equal (v == null)
    }
  }

  test("ALL conversion") {
    val originalValues = ANY_valueGroups.flatten
    val scalaValues = originalValues.map(CypherValue.contents).map(_.orNull)
    val newValues = scalaValues.map {
      case null => null
      case b: java.lang.Boolean => CypherBoolean(b)
      case s: java.lang.String => CypherString(s)
      case l: java.lang.Long => CypherInteger(l)
      case p: Properties => CypherMap(p)
      case (id: EntityId, data: NodeData) => CypherNode(id, data.labels, data.properties)
      case (id: EntityId, data: RelationshipData) => CypherRelationship(id, data)
      case d: java.lang.Double => CypherFloat(d)
      case l: Seq[_] if isPathLike(l) => CypherPath(l.asInstanceOf[Seq[CypherEntityValue]]: _*)
      case l: Seq[_] => CypherList(l.asInstanceOf[Seq[CypherValue]])
    }

    newValues should equal(originalValues)

    originalValues.foreach { v =>
      CypherValue.isComparable(v) should equal (v == null)
    }
  }
}
