package org.opencypher.spark.integration.yelp

object YelpDemo extends App {
  private val emptyArgs: Array[String] = Array.empty

  Part1_YelpImport.main(emptyArgs)
  Part2_YelpTimeSlices.main(emptyArgs)
  Part3_YelpRanking.main(emptyArgs)
  Part4_YelpTrending.main(emptyArgs)
}
