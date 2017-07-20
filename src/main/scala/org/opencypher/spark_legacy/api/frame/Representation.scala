/**
 * Copyright (c) 2016-2017 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.spark_legacy.api.frame

import org.apache.spark.sql.types._
import org.opencypher.spark.api.types._

object Representation {

  def forCypherType(typ: CypherType): Representation = typ.material match {
    case CTInteger => EmbeddedRepresentation(LongType)
    case CTFloat => EmbeddedRepresentation(DoubleType)
    case CTBoolean => EmbeddedRepresentation(BooleanType)
    case CTString => EmbeddedRepresentation(StringType)
    case CTVoid => EmbeddedRepresentation(NullType)
    case _: CTList => BinaryRepresentation
    case _: CTNode | _: CTRelationship | CTPath | CTMap | CTAny | CTNumber | CTWildcard => BinaryRepresentation
  }
}

sealed trait Representation extends Serializable {
  def dataType: DataType
  def isEmbedded: Boolean
}

case object BinaryRepresentation extends Representation {
  def dataType = BinaryType
  def isEmbedded = false
}

final case class EmbeddedRepresentation(dataType: DataType) extends Representation {
  def isEmbedded = true
}

