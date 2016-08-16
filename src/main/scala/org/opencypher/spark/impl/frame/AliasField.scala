package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl.{StdCypherFrame, StdField, StdFrameSignature, StdRuntimeContext}

object AliasField {

  def apply(input: StdCypherFrame[Product])(oldName: Symbol)(newName: Symbol): ProjectFrame = {
    val (newField, newSignature) = input.signature.aliasField(oldName, newName)
    AliasField(input)(newField)(newSignature)
  }

  private final case class AliasField(input: StdCypherFrame[Product])(projectedField: StdField)(sig: StdFrameSignature)
    extends ProjectFrame(projectedField, sig) {

    override def execute(implicit context: StdRuntimeContext): Dataset[Product] = {
      input.run
    }
  }
}
