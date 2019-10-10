import Metrics.Tools

import scala.io.Source

object ManualTests {

  def main(args: Array[String]): Unit = {

    // US:      1101659111430
    // USALL:   47890485652059026823698344598447161988226609934499972
    // Baazizi: 56539106072908298546665520023773392506479484700019806659891398441363832832
    val schema  = JsonSchemaParser.jsFromFile("yelpFML.schema.json")
    println("Precision: " + Metrics.Precision.calculatePrecision(schema))
    println("Count: " + Metrics.Count.countAttributes(schema))
    Tools.getAttributes(schema)

    val t1 = JsonSchemaParser.jsFromFile("t1.txt")
    val t2 = JsonSchemaParser.jsFromFile("t2.txt")
    val t3 = JsonSchemaParser.jsFromFile("t3.txt")
    val t4 = JsonSchemaParser.jsFromFile("t4.txt")

    val a1 = Tools.getAttributes(t1)
    val a2 = Tools.getAttributes(t2)
    val a3 = Tools.getAttributes(t3)
    val a4 = Tools.getAttributes(t4)

    println(a2.subsetOf(a1))
    a1.foreach(a2.remove(_))
    println(a3.subsetOf(a1))
    a1.foreach(a3.remove(_))
    println(a4.subsetOf(a1))
    a1.foreach(a4.remove(_))


    println(a2)
    println(a3)
    println(a4)

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
