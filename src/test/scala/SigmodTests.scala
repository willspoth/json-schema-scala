import java.io.File

import Metrics.Tools

import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger

import scala.io.Source

object SigmodTests {

  def main(args: Array[String]): Unit = {

    val root: Logger  = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[Logger]
    root.setLevel(Level.TRACE)

    def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    val directory = "/home/will/Desktop/yelp-schemas/us"

    // US:      1101659111430
    // USALL:   47890485652059026823698344598447161988226609934499972
    // Baazizi: 56539106072908298546665520023773392506479484700019806659891398441363832832

    val files = getListOfFiles(directory)

    val precision = files.map(file => {
      val schema = JsonSchemaParser.jsFromFile(file.toString)
      "'"+file.getName.split('-')(1) + "':" + Metrics.Precision.calculatePrecision(schema).toString()
    })

    println(s"""{${precision.mkString(",\n")}}""")

    files
      .filter(_.getName.contains("50-"))
      .map(file => {
      val schema = JsonSchemaParser.jsFromFile(file.toString)

      println(Metrics.Validation.calculateValidation(schema,Array(
        //"""{"business_id": "rbdTuDwossp3cmqAKmFSVw", "name": "Mirage Hair Company", "neighborhood": "", "address": "8664 E Shea Blvd, Ste 158", "city": "Scottsdale", "state": "AZ", "postal_code": "85260", "latitude": 33.5851304, "longitude": -111.893705, "stars": 4.5, "review_count": 7, "is_open": 0, "attributes": {"BusinessParking": {"garage": false, "street": false, "validated": false, "lot": true, "valet": false}, "HairSpecializesIn": {"coloring": true, "africanamerican": false, "curly": true, "perms": true, "kids": true, "extensions": false, "asian": false, "straightperms": true}, "BusinessAcceptsCreditCards": true, "RestaurantsPriceRange2": 2, "GoodForKids": true, "ByAppointmentOnly": false, "WheelchairAccessible": true}, "categories": ["Makeup Artists", "Hair Salons", "Beauty & Spas"], "hours": {"Tuesday": "9:00-18:00", "Friday": "9:00-18:00", "Wednesday": "9:00-18:00", "Thursday": "9:00-18:00", "Saturday": "9:00-17:00"}}""",
        """{"business_id": "hDO8bWV_Ua8NxtfpStuuZg", "name": "Initium", "neighborhood": "Brown's Corners", "address": "8241 Woodbine Avenue, Unit 6 & 7", "city": "Markham", "state": "ON", "postal_code": "L3R 2P1", "latitude": 43.8437124813, "longitude": -79.3554014333, "stars": 4.0, "review_count": 73, "is_open": 1, "attributes": {"Alcohol": "full_bar", "HasTV": true, "NoiseLevel": "average", "RestaurantsAttire": "casual", "BusinessAcceptsCreditCards": true, "Ambience": {"romantic": false, "intimate": false, "classy": false, "hipster": false, "touristy": false, "trendy": true, "upscale": false, "casual": false}, "RestaurantsGoodForGroups": true, "Caters": false, "WiFi": "free", "RestaurantsReservations": true, "RestaurantsTableService": true, "RestaurantsTakeOut": true, "GoodForKids": true, "HappyHour": false, "GoodForDancing": false, "BikeParking": true, "OutdoorSeating": false, "HairSpecializesIn": {"perms": true, "coloring": true, "extensions": true, "curly": true, "kids": true}, "RestaurantsPriceRange2": 2, "RestaurantsDelivery": false, "ByAppointmentOnly": true, "GoodForMeal": {"dessert": true, "latenight": true, "lunch": false, "dinner": false, "breakfast": false, "brunch": false}, "BusinessParking": {"garage": false, "street": false, "validated": false, "lot": false, "valet": false}}, "categories": ["Restaurants", "Lounges", "Beauty & Spas", "Hair Salons", "Nightlife", "Bars", "Cafes"], "hours": {"Monday": "10:00-0:00", "Friday": "10:00-0:00", "Wednesday": "10:00-0:00", "Thursday": "10:00-0:00", "Sunday": "10:00-0:00", "Saturday": "10:00-0:00"}}"""
      )))
      ???
      val percent = file.getName.split('-')(1)

      val validaion: Double = if(percent.toInt < 100){
        println(file.toString)
        getListOfFiles("/home/will/Data/sigmod2020Data/yelp/yelp.json.val-"+percent+"-500000").filter(x=>x.getName.charAt(0).equals('p')).map(f => {
          val partValidation = Metrics.Validation.calculateValidation(schema,Source.fromFile(f.toString).getLines.toArray)
          println("\t\t"+f.toString + ": " + partValidation.toString)
          partValidation
          }).reduce(_*_)
      } else {
        getListOfFiles("/home/will/Data/sigmod2020Data/yelp/yelp.json.val-10-500000").filter(x=>x.getName.charAt(0).equals('p')).map(f => {
          val partValidation = Metrics.Validation.calculateValidation(schema,Source.fromFile(f.toString).getLines.toArray)
          println("\t\t"+f.toString + ": " + partValidation.toString)
          partValidation
        }).reduce(_*_)
      }
      (file.getName,validaion,percent.toInt)
    }).sortBy(_._3).foreach(x => println(x._1 + ": " + x._2.toString))



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
