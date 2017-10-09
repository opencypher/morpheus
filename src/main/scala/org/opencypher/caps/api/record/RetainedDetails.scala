package org.opencypher.caps.api.record

object RetainedDetails {
  implicit val default: RetainedDetails =
    RetainedDetails(nodeLabels = false, properties = false)

  def current(implicit options: RetainedDetails): RetainedDetails =
    options
}

final case class RetainedDetails(nodeLabels: Boolean, properties: Boolean) {
  def withLabels: RetainedDetails = copy(nodeLabels = true)
  def withProperties: RetainedDetails = copy(properties = true)
}
