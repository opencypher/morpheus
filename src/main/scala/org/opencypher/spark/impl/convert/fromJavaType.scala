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
package org.opencypher.spark.impl.convert

import org.opencypher.spark.api.types._
import org.opencypher.spark.impl.exception.Raise

object fromJavaType extends Serializable {

  def apply(v: AnyRef): CypherType = v match {
    case null => CTVoid
    case _: String => CTString
    case _: java.lang.Byte => CTInteger
    case _: java.lang.Short => CTInteger
    case _: java.lang.Integer => CTInteger
    case _: java.lang.Long => CTInteger
    case _: java.lang.Float => CTFloat
    case _: java.lang.Double => CTFloat
    case _: java.lang.Boolean => CTBoolean
    case x => Raise.invalidArgument("instance of a CypherValue", s"${x.getClass}")
  }
}
