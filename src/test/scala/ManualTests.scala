import play.api.libs.json.Json

import scala.io.Source

object ManualTests {

  def main(args: Array[String]): Unit = {
    val fileName: String = "/home/will/Data/jsonData/test"

    val schema  = JsonSchemaParser.jsFromFile(fileName.split("/").last+".schema.json")

//    println(
//      Json.prettyPrint(
//        Json.parse(schema.toString)
//      )
//    )

    println("Precision: " + Metrics.Precision.calculatePrecision(schema))
    println("Grouping: " + Metrics.Grouping.calculateGrouping(schema))

    val rows = Source.fromFile(fileName+".json").getLines.foreach( r => {
      println(
        Metrics.Validation.validateRow(
          schema,
          Types.Json.shred(r)
        )
      )
    })

    println("done")
  }
}
