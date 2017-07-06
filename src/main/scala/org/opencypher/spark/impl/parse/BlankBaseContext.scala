package org.opencypher.spark.impl.parse

import org.neo4j.cypher.internal.frontend.v3_3.phases._
import org.neo4j.cypher.internal.frontend.v3_3.{AstRewritingMonitor, CypherException, InputPosition}

import scala.reflect.ClassTag

abstract class BlankBaseContext extends BaseContext {
  override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING
  override def notificationLogger: InternalNotificationLogger = devNullLogger
  override def exceptionCreator: (String, InputPosition) => CypherException = (_, _) => null

  override def monitors: Monitors = new Monitors {
    override def newMonitor[T <: AnyRef : ClassTag](tags: String*): T = {
      new AstRewritingMonitor {
        override def abortedRewriting(obj: AnyRef): Unit = ()
        override def abortedRewritingDueToLargeDNF(obj: AnyRef): Unit = ()
      }
    }.asInstanceOf[T]

    override def addMonitorListener[T](monitor: T, tags: String*): Unit = ()
  }
}
