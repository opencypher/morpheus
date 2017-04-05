package org.opencypher.spark_legacy.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark_legacy.impl.{StdCypherFrame, StdField, StdFrameSignature, StdRuntimeContext}

object AliasField extends FrameCompanion {

  def apply(input: StdCypherFrame[Product])(oldName: Symbol)(newName: Symbol): ProjectFrame = {
    val (newField, newSignature) = input.signature.aliasField(oldName -> newName)
    AliasField(input)(newField)(newSignature)
  }

  private final case class AliasField(input: StdCypherFrame[Product])(projectedField: StdField)(sig: StdFrameSignature)
    extends ProjectFrame(projectedField, sig) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      input.run
    }
  }
}
