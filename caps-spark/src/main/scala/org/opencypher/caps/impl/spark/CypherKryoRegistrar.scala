/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
package org.opencypher.caps.impl.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.{KryoRegistrator => SparkKryoRegistrar}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, NullsFirst, NullsLast, UnsafeRow}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._
import org.neo4j.driver.internal._
import org.neo4j.driver.internal.value._
import org.neo4j.driver.v1.Value
import org.opencypher.caps.api.schema._
import org.opencypher.caps.api.types._
import org.opencypher.caps.api.value.CypherValue._
import org.opencypher.caps.api.value._

import scala.collection.immutable.TreeMap
import scala.language.existentials

class CypherKryoRegistrar extends SparkKryoRegistrar {

  private val registeredClasses = Seq(
    classOf[CypherMap],
    classOf[CypherInteger],
    classOf[CypherFloat],
    classOf[CypherNumber[Long]],
    classOf[CypherNumber[Double]],
    classOf[CypherString],
    classOf[CypherBoolean],
    CypherNull.getClass,
    classOf[CAPSNode],
    classOf[CAPSRelationship],
    classOf[CypherNode[Long]],
    classOf[CypherRelationship[Long]],
    classOf[CypherMap],
    Class.forName("org.opencypher.caps.api.value.CypherValue$CypherList"),
    Class.forName("scala.collection.immutable.MapLike$$anon$2"),
    Class.forName("scala.collection.mutable.WrappedArray$ofRef"),
    Class.forName("scala.Predef$$anon$1"),
    classOf[Array[String]],
    classOf[Array[Long]],
    classOf[Array[Integer]],
    classOf[Array[Int]],
    classOf[Array[Double]],
    classOf[Array[Float]],
    classOf[Array[CypherInteger]],
    classOf[Array[CypherFloat]],
    classOf[Array[CypherNumber[Long]]],
    classOf[Array[CypherNumber[Double]]],
    classOf[Array[CypherString]],
    classOf[Array[CypherBoolean]],
    classOf[Array[CypherMap]],
    classOf[Schema],
    classOf[ImpliedLabels],
    classOf[LabelCombinations],
    classOf[RelTypePropertyMap],
    classOf[LabelPropertyMap],
    classOf[CypherType],
    CTWildcard.getClass,
    CTWildcard.nullable.getClass,
    CTAny.getClass,
    CTAnyOrNull.getClass,
    CTBoolean.getClass,
    CTBooleanOrNull.getClass,
    CTString.getClass,
    CTStringOrNull.getClass,
    CTInteger.getClass,
    CTIntegerOrNull.getClass,
    CTFloat.getClass,
    CTFloatOrNull.getClass,
    CTNode.getClass,
    CTNodeOrNull.getClass,
    CTRelationship.getClass,
    CTRelationshipOrNull.getClass,
    CTMap.getClass,
    CTMapOrNull.getClass,
    CTMap.getClass,
    CTMapOrNull.getClass,
    CTPath.getClass,
    CTPathOrNull.getClass,
    CTList.getClass,
    CTListOrNull.getClass,

    classOf[scala.collection.mutable.WrappedArray.ofRef[AnyRef]],
    classOf[Class[AnyRef]],
    classOf[Array[String]],
    classOf[Array[Array[Byte]]],
    classOf[Array[java.lang.Object]],
    classOf[java.util.ArrayList[AnyRef]],
    classOf[java.util.HashMap[AnyRef, AnyRef]],

    Class.forName("scala.reflect.ClassTag$$anon$1"),
    Class.forName("org.apache.spark.sql.execution.joins.LongHashedRelation"),
    Class.forName("org.apache.spark.sql.execution.joins.LongToUnsafeRowMap"),
    Class.forName("org.apache.spark.sql.execution.columnar.CachedBatch"),
    Class.forName("org.apache.spark.sql.catalyst.expressions.Cast"),
    Class.forName("org.apache.spark.sql.catalyst.expressions.GenericInternalRow"),
    Class.forName("org.apache.spark.sql.catalyst.expressions.Literal"),
    Class.forName("org.apache.spark.sql.catalyst.expressions.ToDate"),
    Class.forName("org.apache.spark.sql.catalyst.expressions.UnixTimestamp"),
    Class.forName("org.apache.spark.unsafe.types.UTF8String"),
    Class.forName("org.apache.spark.sql.execution.joins.UnsafeHashedRelation"),
    classOf[Array[InternalRow]],
    classOf[UnsafeRow],

    classOf[TreeMap[String, Any]],
    implicitly[Ordering[String]].getClass,
    implicitly[Ordering[Long]].getClass,
    implicitly[Ordering[Int]].getClass,
    implicitly[Ordering[Boolean]].getClass,
    implicitly[Ordering[Float]].getClass,
    TypeUtils.getInterpretedOrdering(BinaryType).getClass,
    Class.forName("scala.math.Ordering$$anon$4"),
    Class.forName("org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering"),
    Class.forName("org.apache.spark.sql.catalyst.trees.Origin"),
    classOf[org.apache.spark.sql.catalyst.expressions.SortOrder],
    classOf[org.apache.spark.sql.catalyst.expressions.BoundReference],
    classOf[Array[org.apache.spark.sql.catalyst.expressions.SortOrder]],
    Ascending.getClass,
    Descending.getClass,
    NullsFirst.getClass,
    NullsLast.getClass,

    IntegerType.getClass,
    LongType.getClass,
    StringType.getClass,
    BinaryType.getClass,
    DoubleType.getClass,
    BooleanType.getClass,
    DateType.getClass,
    TimestampType.getClass,

    Class.forName("scala.collection.immutable.RedBlackTree$Tree"),
    Class.forName("scala.collection.immutable.RedBlackTree$BlackTree"),
    Class.forName("scala.collection.immutable.RedBlackTree$RedTree"),
    Class.forName("scala.collection.immutable.Map$EmptyMap$"),
    Class.forName("scala.collection.immutable.Set$EmptySet$"),
    Class.forName("scala.collection.immutable.MapLike$ImmutableDefaultKeySet"),

    classOf[InternalRecord],
    classOf[InternalPath],
    classOf[InternalPair[AnyRef, AnyRef]],
    classOf[InternalEntity],
    classOf[InternalNode],
    classOf[InternalRelationship],
    classOf[Value],
    classOf[StringValue],
    classOf[BooleanValue],
    classOf[FloatValue],
    classOf[IntegerValue],
    classOf[MapValue],
    classOf[NullValue],
    classOf[PathValue],
    classOf[NodeValue],
    classOf[RelationshipValue]
  )

  override def registerClasses(kryo: Kryo): Unit = {
    import com.twitter.chill.toRich

    kryo.registerClasses(registeredClasses)
  }
}
