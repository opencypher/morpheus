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
package org.opencypher.spark.demo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.{KryoRegistrator => SparkKryoRegistrar}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.opencypher.spark.api.schema.{ImpliedLabels, LabelCombinations, PropertyKeyMap, Schema}
import org.opencypher.spark.api.value._

import scala.collection.PrivateCollectionClasses
import scala.collection.immutable.TreeMap
import scala.language.existentials

class CypherKryoRegistrar extends SparkKryoRegistrar {

    private val registeredClasses = Seq(
      classOf[CypherValue],
      classOf[CypherInteger],
      classOf[CypherFloat],
      classOf[CypherNumber],
      classOf[CypherString],
      classOf[CypherBoolean],
      classOf[CypherMap],
      classOf[CypherNode],
      classOf[CypherRelationship],
      classOf[CypherPath],
      classOf[CypherList],
      classOf[Properties],
      classOf[Array[CypherNode]],
      classOf[Array[CypherRelationship]],

      classOf[TreeMap[String, Any]],
      Ordering.String.getClass,
      classOf[scala.collection.mutable.WrappedArray.ofRef[AnyRef]],
      classOf[Class[AnyRef]],
      classOf[Array[String]],
      classOf[Array[Array[Byte]]],

      Class.forName("scala.reflect.ClassTag$$anon$1"),
      Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
      Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
      Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"),
      Class.forName("org.apache.spark.sql.catalyst.expressions.GenericInternalRow"),
      Class.forName("org.apache.spark.unsafe.types.UTF8String"),
      Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),

      classOf[Schema],
      classOf[ImpliedLabels],
      classOf[LabelCombinations],
      classOf[PropertyKeyMap],
      Class.forName("scala.collection.immutable.Map$EmptyMap$"),
      Class.forName("scala.collection.immutable.Set$EmptySet$"),

      classOf[Array[java.lang.Object]],
      classOf[Array[InternalRow]],
      classOf[UnsafeRow]
    )

    override def registerClasses(kryo: Kryo): Unit = {
      registeredClasses.foreach(kryo.register)
      PrivateCollectionClasses.registeredClasses.foreach(kryo.register)
    }
  }
