package org.opencypher.spark.impl

package object util {
  type SuccessfulAdditiveUpdateResult[T] = SuccessfulUpdateResult[T] with AdditiveUpdateResult[T]
}
