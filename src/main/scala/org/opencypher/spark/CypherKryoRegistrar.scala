package org.opencypher.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.{KryoRegistrator => SparkKryoRegistrar}
import org.opencypher.spark.api.value._

import scala.collection.immutable.TreeMap

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

    classOf[TreeMap[String, Any]],
    Ordering.String.getClass,
    classOf[scala.collection.mutable.WrappedArray.ofRef[AnyRef]],
    classOf[Class[AnyRef]]
  )

  override def registerClasses(kryo: Kryo): Unit = {
    registeredClasses.foreach(kryo.register)
  }
}
