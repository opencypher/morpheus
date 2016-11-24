package org.opencypher.spark.benchmark

import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.opencypher.spark.api.value._

object Converters {
  import scala.collection.JavaConverters._

  object cypherValue extends (Any => CypherValue) {
    override def apply(v: Any): CypherValue = v match {
      case v: String => CypherString(v)
      case v: java.lang.Byte => CypherInteger(v.toLong)
      case v: java.lang.Short => CypherInteger(v.toLong)
      case v: java.lang.Integer => CypherInteger(v.toLong)
      case v: java.lang.Long => CypherInteger(v)
      case v: java.lang.Float => CypherFloat(v.toDouble)
      case v: java.lang.Double => CypherFloat(v)
      case v: java.lang.Boolean => CypherBoolean(v)
      case v: Array[_] => CypherList(v.map(cypherValue))
      case null => null
      case x => throw new IllegalArgumentException(s"Unexpected property value: $x")
    }

  }

  case object internalNodeToCypherNode extends (InternalNode => CypherNode) {

    override def apply(michael: InternalNode): CypherNode = {
      val props = michael.asMap().asScala.mapValues(cypherValue)
      val properties = Properties(props.toSeq:_*)
      CypherNode(michael.id(), michael.labels().asScala.toArray, properties)
    }
  }

  case object internalRelationshipToCypherRelationship extends (InternalRelationship => CypherRelationship) {

    override def apply(michael: InternalRelationship): CypherRelationship = {
      val props = michael.asMap().asScala.mapValues(cypherValue)
      val properties = Properties(props.toSeq:_*)
      CypherRelationship(michael.id(), michael.startNodeId(), michael.endNodeId(), michael.`type`(), properties)
    }
  }

  case object internalNodeToAccessControlNode extends (InternalNode => AccessControlNode) {
    override def apply(michael: InternalNode): AccessControlNode = {
      // TODO: properties
      AccessControlNode(michael.id(),
        michael.hasLabel(AccessControlSchema.labels(0)),
        michael.hasLabel(AccessControlSchema.labels(1)),
        michael.hasLabel(AccessControlSchema.labels(2)),
        michael.hasLabel(AccessControlSchema.labels(3)),
        michael.hasLabel(AccessControlSchema.labels(4)),
        michael.hasLabel(AccessControlSchema.labels(5))
      )
    }
  }

  case object internalRelToAccessControlRel extends (InternalRelationship => AccessControlRelationship) {
    override def apply(michael: InternalRelationship): AccessControlRelationship = {
      // TODO: properties
      AccessControlRelationship(michael.id(), michael.startNodeId(), michael.endNodeId(), michael.`type`())
    }
  }
}

object AccessControlSchema {
  val labels = IndexedSeq("Account", "Administrator", "Company", "Employee", "Group", "Resource")

  def labelIndex(label: String) = label match {
    case "Account" => 1
    case "Administrator" => 2
    case "Company" => 3
    case "Employee" => 4
    case "Group" => 5
    case "Resource" => 6
    case x => throw new IllegalArgumentException(s"No such label $x")
  }
}

case class AccessControlNode(id: Long, account: Boolean, administrator: Boolean, company: Boolean, employee: Boolean, group: Boolean, resource: Boolean)
case class AccessControlRelationship(id: Long, startId: Long, endId: Long, typ: String)
