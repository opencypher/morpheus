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
package org.opencypher.caps.impl.spark.io.neo4j.external

import org.apache.spark.sql.types
import org.apache.spark.sql.types._
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.v1.types.{Type, TypeSystem}

object CypherTypes {
  val INTEGER: LongType.type = types.LongType
  val FlOAT: DoubleType.type = types.DoubleType
  val STRING: StringType.type = types.StringType
  val BOOLEAN: BooleanType.type = types.BooleanType
  val NULL: NullType.type = types.NullType

  def apply(typ:String): DataType = typ.toUpperCase match {
    case "LONG" => INTEGER
    case "INT" => INTEGER
    case "INTEGER" => INTEGER
    case "FLOAT" => FlOAT
    case "DOUBLE" => FlOAT
    case "NUMERIC" => FlOAT
    case "STRING" => STRING
    case "BOOLEAN" => BOOLEAN
    case "BOOL" => BOOLEAN
    case "NULL" => NULL
    case _ => STRING
  }

  def toSparkType(typeSystem : TypeSystem, typ : Type): org.apache.spark.sql.types.DataType =
    if (typ == typeSystem.BOOLEAN()) CypherTypes.BOOLEAN
    else if (typ == typeSystem.STRING()) CypherTypes.STRING
    else if (typ == typeSystem.INTEGER()) CypherTypes.INTEGER
    else if (typ == typeSystem.FLOAT()) CypherTypes.FlOAT
    else if (typ == typeSystem.NULL()) CypherTypes.NULL
    else CypherTypes.STRING

  def field(keyType: (String, Type)): StructField = {
    StructField(keyType._1, CypherTypes.toSparkType(InternalTypeSystem.TYPE_SYSTEM, keyType._2))
  }

  def schemaFromNamedType(schemaInfo: Seq[(String, String)]): StructType = {
    val fields = schemaInfo.map(field =>
      StructField(field._1, CypherTypes(field._2), nullable = true) )
    StructType(fields)
  }

  def schemaFromDataType(schemaInfo: Seq[(String, types.DataType)]): StructType = {
    val fields = schemaInfo.map(field =>
      StructField(field._1, field._2, nullable = true) )
    StructType(fields)
  }
}
