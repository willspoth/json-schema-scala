import play.api.libs.json.Json

object ManualTests {

  def main(args: Array[String]): Unit = {
    val v  = JsonSchemaParser.jsFromFile("yelp.js")

    println(
      Json.prettyPrint(
        Json.parse(v.toString)
      )
    )
  }
}
