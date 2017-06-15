package org.opencypher.spark.api.util

import scala.language.{higherKinds, implicitConversions}

trait Verifiable {

  type Self
  type VerifiedSelf <: Verified[Self]

  def verify: VerifiedSelf
}

trait Verified[+V] {
  def v: V
}

object Verified {
  implicit def lift[IN, OUT <: Verified[IN]](input: Verifiable { type Self = IN; type VerifiedSelf = OUT }): OUT =
    input.verify
}
