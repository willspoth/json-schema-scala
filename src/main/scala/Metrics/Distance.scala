package Metrics

import java.io.File

import JsonParser.JsonSchemaParser
import Types.JsonSchema
import Types.JsonSchema.{JSA_type, JSS, JsonSchemaType, Obj}

import scala.io.Source
import util.Utils.glob

import scala.collection.mutable

object Distance {

  private def computeDistance(testSet: mutable.Set[String], groundTruthSet: mutable.Set[String], comparisonType: String): (mutable.Set[String],Int) = {
    if(comparisonType.equals("symmetric")){
      val diff = groundTruthSet.diff(testSet).union(testSet.diff(groundTruthSet))
      (diff,diff.size)
    } else if(comparisonType.equals("diff")){
      val diff = testSet.diff(groundTruthSet)
      (diff,diff.size)
    } else if(comparisonType.equals("sub")){
      if(groundTruthSet.subsetOf(testSet)) (null,1) else (null,0)
    } else {
      throw new Exception(s"Unknown comparison: $comparisonType")
    }
  }

  def distance(testSchema: JSS, groundTruthSchema: JSS, comparisonType: String): Int = {
    val groundTruthSet = scala.collection.mutable.Set[String]()
    JSStoSet("$",groundTruthSchema,groundTruthSet)
    testSchema.anyOf match {
      case Some(schemas) =>
        val results = schemas.value.map(schema => {
          val schemaSet = scala.collection.mutable.Set[String]()
          JSStoSet("$",schema,schemaSet)
          computeDistance(schemaSet,groundTruthSet,comparisonType)
        }).sortBy(_._2)
        val delta = results.head._1
        results.head._2
      case None =>
        val schemaSet = scala.collection.mutable.Set[String]()
        JSStoSet("$",testSchema,schemaSet)
        val res = computeDistance(schemaSet,groundTruthSet,comparisonType)
        res._2
    }
  }


  def JSStoSet(name: String, schema: JSS, attributeSet: scala.collection.mutable.Set[String]): Unit = {
    schema.anyOf match {
      case Some(a) => a.value.map(x => JSStoSet(name, x, attributeSet))
      case None =>
    }
    schema.oneOf match {
      case Some(a) => a.value.map(x => JSStoSet(name, x, attributeSet))
      case None =>
    }

    schema.properties match {
      case Some(prop) =>
        prop.value.foreach(p => JSStoSet(name+"."+p._1,p._2,attributeSet))
        attributeSet.add(name)
      case None => // leaf
        schema.items match {
          case Some(items) =>
            JSStoSet(name+"[*]", items.value, attributeSet)
            attributeSet.add(name+"[*]")
          case None =>
            attributeSet.add(name)
        }
    }

  }

  def MultiArrayOfObj(schema: JSS, lastType: JsonSchemaType, passedThroughAnyOf: Boolean): Unit = {
    schema.anyOf match {
      case Some(a) => a.value.map(x => MultiArrayOfObj(x,lastType,true))
      case None =>
    }
    schema.oneOf match {
      case Some(a) => a.value.map(x => MultiArrayOfObj(x, lastType, passedThroughAnyOf))
      case None =>
    }
    schema.`type` match {
      case Some(t) =>
        if(t.value.equals(JsonSchema.Obj) && lastType.equals(JsonSchema.Arr) && passedThroughAnyOf == true){
          println("fml")
          ???
        }
      case None =>
    }
    schema.properties match {
      case Some(prop) =>
        prop.value.foreach(p => MultiArrayOfObj(p._2,schema.`type`.get.value,false))
      case None => // leaf
    }
    schema.items match {
      case Some(items) =>
        MultiArrayOfObj(items.value,schema.`type`.get.value,false)
      case None =>
    }
  }

  def calcDistance(testPath: String, groundTruthPath: String, compType: String): Unit = {
    val testFiles: List[File] = glob(testPath)
    val groundTruthFiles: List[File] = glob(groundTruthPath)
    val results = scala.collection.mutable.ListBuffer[(String,String,Int)]()

    testFiles.foreach(testFile => {
      val testIter = Source.fromFile(testFile).getLines()
      testIter.next() // skip info line
      val testSchema: JSS = JsonSchemaParser.jsFromString(testIter.next())
//      if(testFile.getName.contains("bimax")){
//        MultiArrayOfObj(testSchema,Obj,false)
//        println("passed")
//      }
      groundTruthFiles.foreach(groundTruthFile => {
        val groundTruthIter = Source.fromFile(groundTruthFile).getLines()
        groundTruthIter.next()
        val groundTruthSchema: JSS = JsonSchemaParser.jsFromString(groundTruthIter.next())
        if(!testFile.getName.equals(groundTruthFile.getName))
          results.append((testFile.getName.split("\\.")(1),groundTruthFile.getName.split("\\.").head,distance(testSchema, groundTruthSchema, compType)))
      })
    })

    if(compType.equals("symmetric") || compType.equals("diff")){
      println(results.filter(_._1.contains("baazizi")).map(x => s"${x._3}").mkString(" & "))
      println(results.filter(_._1.contains("bimax")).map(x => s"${x._3}").mkString(" & "))
      println(results.filter(_._1.contains("kmeans")).map(x => s"${x._3}").mkString(" & "))
      println(results.filter(_._1.contains("hier")).map(x => s"${x._3}").mkString(" & "))
    }
    else if(compType.equals("sub")){
      println(results.filter(x => if(x._3 == 1) true else false ).map(x => s"${x._2} subset of ${x._1}").mkString("\n"))
    }
  }

}
