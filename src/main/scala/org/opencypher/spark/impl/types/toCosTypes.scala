package org.opencypher.spark.impl.types

import org.opencypher.spark.api.CypherType
import org.neo4j.cypher.internal.frontend.v3_2.{symbols => neo4j}
import org.opencypher.spark.api.types._

object toCosTypes extends (neo4j.CypherType => CypherType) {
  override def apply(in: neo4j.CypherType): CypherType = in match {
    case neo4j.CTNumber => CTNumber
    case neo4j.CTInteger => CTInteger
    case neo4j.CTFloat => CTFloat
    case neo4j.CTBoolean => CTBoolean
    case neo4j.CTString => CTString
    case neo4j.CTBoolean => CTBoolean
    case neo4j.CTMap => CTMap
    case neo4j.CTNode => CTNode
    case neo4j.CTRelationship => CTRelationship
    case neo4j.ListType(inner) => CTList(toCosTypes(inner))
    case x => throw new UnsupportedOperationException(s"No support for type $x")
  }
}
