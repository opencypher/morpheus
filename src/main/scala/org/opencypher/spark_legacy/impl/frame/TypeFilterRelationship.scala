package org.opencypher.spark_legacy.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.value.CypherRelationship
import org.opencypher.spark_legacy.impl.{StdCypherFrame, StdRuntimeContext}

object TypeFilterRelationship extends FrameCompanion {

  def apply(input: StdCypherFrame[CypherRelationship])(types: Set[String]): StdCypherFrame[CypherRelationship] = {
    TypeFilterRelationship(input)(types)
  }

  private final case class TypeFilterRelationship(input: StdCypherFrame[CypherRelationship])(types: Set[String])
    extends StdCypherFrame[CypherRelationship](input.signature) {

    override def execute(implicit context: StdRuntimeContext): Dataset[CypherRelationship] = {
      val in = input.run
      val out = in.filter(typeFilter(types))
      out
    }
  }

  private final case class typeFilter(types: Set[String]) extends (CypherRelationship => Boolean) {

    override def apply(relationship: CypherRelationship): Boolean = {
      types.forall(t => CypherRelationship.relationshipType(relationship).exists(types))
    }
  }
}
