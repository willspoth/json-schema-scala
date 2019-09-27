import play.api.libs.json.Json

import scala.io.Source

object ManualTests {

  def main(args: Array[String]): Unit = {

    // US:      1101659111430
    // USALL:   47890485652059026823698344598447161988226609934499972
    // Baazizi: 56539106072908298546665520023773392506479484700019806659891398441363832832
    val schema  = JsonSchemaParser.jsFromFile("yelpUS.schema.json")
    println("Precision: " + Metrics.Precision.calculatePrecision(schema))

//    val fileName: String = "/home/will/Data/jsonData/test"
//
//    val schema  = JsonSchemaParser.jsFromFile(fileName.split("/").last+".schema.json")
//
////    println(
////      Json.prettyPrint(
////        Json.parse(schema.toString)
////      )
////    )
//
//    println("Precision: " + Metrics.Precision.calculatePrecision(schema))
//    println("Grouping: " + Metrics.Grouping.calculateGrouping(schema))
//
//    val rows = Source.fromFile(fileName+".json").getLines.foreach( r => {
//      println(
//        Metrics.Validation.validateRow(
//          schema,
//          Types.Json.shred(r)
//        )
//      )
//    })
//
//    println("done")
  }
}
