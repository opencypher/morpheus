package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.impl.{StdCypherFrame, StdField, StdFrameSignature, StdRuntimeContext}

object AliasField {

  def apply(input: StdCypherFrame[Product], field: StdField)(newName: Symbol): ProjectFrame = {
    val (newField, newSignature) = input.signature.aliasField(field.sym, newName)
    new AliasField(input, newField)(newSignature)
  }

  private final class AliasField(input: StdCypherFrame[Product], projectedField: StdField)(sig: StdFrameSignature) extends ProjectFrame(projectedField, sig) {

    override def run(implicit context: StdRuntimeContext): Dataset[Product] = {
      alias(input.run, context.productEncoder(slots))
    }
  }

}
