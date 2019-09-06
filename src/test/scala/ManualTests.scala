import play.api.libs.json.Json

object ManualTests {

  def main(args: Array[String]): Unit = {
    val v  = JsonSchemaParser.jsFromFile("sample.schema.json")

    println(
      Json.prettyPrint(
        Json.parse(v.toString)
      )
    )

    println("Precision: " + Metrics.Precision.calculatePrecision(v))

  }
}
