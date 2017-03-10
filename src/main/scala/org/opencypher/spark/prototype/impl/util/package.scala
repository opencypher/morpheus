package org.opencypher.spark.prototype.impl

package object util {
  type SuccessfulAdditiveUpdateResult[T] = SuccessfulUpdateResult[T] with AdditiveUpdateResult[T]
}
