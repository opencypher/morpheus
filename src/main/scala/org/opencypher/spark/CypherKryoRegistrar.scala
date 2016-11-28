
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.{KryoRegistrator => SparkKryoRegistrar}
import org.opencypher.spark.api.value._

import scala.collection.immutable.TreeMap

package org.opencypher.spark {

  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.catalyst.expressions.UnsafeRow
  import org.opencypher.spark.benchmark.{AccessControlNode, AccessControlRelationship}

  import scala.collection.PrivateCollectionClasses

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


      classOf[Array[java.lang.Object]],
      classOf[Array[InternalRow]],
      classOf[UnsafeRow],
      classOf[AccessControlNode],
      classOf[AccessControlRelationship],
      classOf[Array[AccessControlNode]],
      classOf[Array[AccessControlRelationship]]
    )

    override def registerClasses(kryo: Kryo): Unit = {
      registeredClasses.foreach(kryo.register)
      PrivateCollectionClasses.registeredClasses.foreach(kryo.register)
    }
  }
}
