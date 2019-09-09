import play.api.libs.json.Json

object ManualTests {

  def main(args: Array[String]): Unit = {
    val v  = JsonSchemaParser.jsFromFile("trumpTwitter.schema.json")

    println(
      Json.prettyPrint(
        Json.parse(v.toString)
      )
    )

    println("Precision: " + Metrics.Precision.calculatePrecision(v))
    println("Grouping: " + Metrics.Grouping.calculateGrouping(v))


  }
}
