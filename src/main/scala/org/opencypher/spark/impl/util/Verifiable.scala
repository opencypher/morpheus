package org.opencypher.spark.impl.util

trait Verifiable {

  type Self
  type VerifiedSelf <: Verified[Self]

  def verify: VerifiedSelf
}
