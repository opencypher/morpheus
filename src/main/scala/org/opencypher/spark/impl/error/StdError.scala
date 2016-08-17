package org.opencypher.spark.impl.error

abstract class StdError(val detail: String)(implicit private val info: StdErrorInfo)
  extends AssertionError(info.message(detail)) {
  self: Product with Serializable =>

  def enclosingContextFullName = info.enclosing.value
  def contextFullName = info.fullName.value
  def contextPackage = info.pkg.value
  def contextName = info.name.value
  def sourceLocationText = info.locationText
  def sourceLocation = info.location
}
