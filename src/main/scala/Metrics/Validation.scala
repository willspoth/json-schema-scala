package Metrics

import java.io.FileWriter

import Types.Json.{JE_Array, JE_Boolean, JE_Empty_Array, JE_Empty_Object, JE_Null, JE_Numeric, JE_Object, JE_String, JsonExplorerType}
import Types.JsonSchema.{Arr, Bool, JSA_additionalProperties, JSA_required, JSS, Null, Num, Obj, Str}
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer


object Validation {

  //val logger = Logger.getLogger(this.getClass)
  // returns: count passes, total rows, total missing attributes
  def calculateValidation(schema: JSS, rows: RDD[String]): (Double,Double,Double) = {
    val res: RDD[(Double,List[scala.collection.mutable.HashMap[String,Int]])] = rows.zipWithIndex().map(x => {
      val issues: List[scala.collection.mutable.HashMap[String,Int]] = schema.anyOf match {
        case Some(anyOf) =>
          anyOf.value.map(x => scala.collection.mutable.HashMap[String,Int]()).toList
        case None => // single entity
          List(scala.collection.mutable.HashMap[String,Int]())
      }
      if(validateRow(schema,Types.Json.shred(x._1),issues,true)) {
        (1.0,null)
      } else {
        (0.0,issues)
      }
    })

    val validatedRows = res.map(_._1).reduce(_+_)

    if(res.filter(_._1 == 0.0).count() == 0.0){
      return (validatedRows,rows.count().toDouble,0.0)
    }

    val imTired = res.filter(_._1 == 0.0).map(_._2).reduce{case(left,right) => {
        right.zipWithIndex.foreach { // for every entity
          case (rig, idx) => { // for every issues map
            rig.foreach(r => // for every issue
              left(idx).put(r._1, left(idx).getOrElse(r._1, 0) + r._2)
            )
          }
        }
      left
    }}

    val totalEdits = imTired.map(_.size).reduce(_+_)

    (validatedRows,rows.count().toDouble,totalEdits)
  }

  def validateRow(schema: JSS, attribute: Types.Json.JsonExplorerType,issues: List[scala.collection.mutable.HashMap[String,Int]],first: Boolean,depth: Int = 0,name: String = "$", strongCheck: Boolean = false): Boolean = {
    //logger.trace(("\t"*depth) + name + ": validating attribute")

    schema.anyOf match {
      case Some(anyOf) =>
        if(first) {
          val newIssues = anyOf.value.map(x => scala.collection.mutable.HashMap[String,Int]()).toList
          val res = anyOf.value.zipWithIndex.map(s => {
            validateRow(s._1, attribute, List(newIssues(s._2)),false,depth + 1, name)
          })
          if(res.forall(!_)){ // all are false
            val idxMin = newIssues.map(x => if(x.size == 0) 1 else x.size).zipWithIndex.map(x => (x._1,x._2)).min._2
            newIssues(idxMin).foreach(x => issues(idxMin).put(x._1,x._2))
          }

          !res.forall(!_)
        } else {
          !anyOf.value.zipWithIndex.map(s => validateRow(s._1, attribute,issues,false, depth + 1, name)).forall(!_)
        }
      case None =>

        if (!schema.oneOf.equals(None)){
          val matches = schema.oneOf.get.value.map( s => validateRow(s,attribute,issues,false,depth+1,name)).map(x => if (x) 1 else 0)
            .reduce(_+_)
          if (matches > 1) {
            //logger.trace(("\t"*depth) + name + ": oneOf " + matches.toString + " matches found, expected 1")
          }
          matches >= 1 // oneof only used for types now, little hack
        } else {

          attribute match {
            case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array =>
              val typeCheck = compareTypes(schema, attribute)
              if (typeCheck) {
                //logger.trace(("\t"*depth) + name + ": " + attribute.getClass + " found and ok")
              } else {
                if(strongCheck)
                  issues.head.put("type_check",issues.head.getOrElse("type_check",0)+1)
                //logger.debug(("\t"*depth) + name + ": " + attribute.getClass + " found : " + schema.`type`.getOrElse(None).toString)
              }
              typeCheck

            case JE_Empty_Object =>
              val typeCheck = compareTypes(schema, attribute)
              if (typeCheck) {
                //logger.trace(("\t"*depth) + name + ": " + attribute.getClass + " found and ok")
              } else {
                //issues.put("empty_obj_check",issues.getOrElse("empty_obj_check",0)+1)
                //logger.debug(("\t"*depth) + name + ": " + attribute.getClass + " found : " + schema.`type`.getOrElse(None).toString)
              }

              typeCheck && (schema.additionalProperties.getOrElse(JSA_additionalProperties(false)).value || schema.required.getOrElse(JSA_required(Set[String]())).value.isEmpty)

            case JE_Object(m) =>
              val passesMaxPropertiesCheck = schema.maxProperties match { // check on constraint
                case None => true
                case Some(max) =>
                  if (m.size > max.value) {
                    //logger.debug(("\t"*depth) + name + " maxProperties: " + max.value.toString + " found size: " + m.size.toString)
                    if(strongCheck)
                      issues.head.put("object_max",issues.head.getOrElse("object_max",0)+1)
                    false
                  } else {
                    true
                  }
              }

              val passesRequiredCheck: Boolean = schema.required match {
                case Some(req) =>
                  if ((req.value.size > 0)) req.value.map(t => {
                    if (!m.contains(t)) {
                      //logger.debug(("\t"*depth) + name + " does not contain required attribute: " + t)
                    }
                    m.contains(t)
                  }).reduce(_ && _)
                  else true
                case None => true
              }

              if (passesRequiredCheck == false) {
                //issues.put("required_check",issues.getOrElse("required_check",0)+1)
              }

              val additionalProperties: Boolean = schema.additionalProperties match {
                case Some(b) => b.value
                case None => false
              }

              val allAttributesPass: Boolean = m.map { case (n, t) => {
                schema.properties match {
                  case Some(s) =>
                    s.value.get(n) match {
                      case Some(v) =>
                        validateRow(v, t,issues,false,depth+1, name+"."+n)
                      case None =>
                        //logger.debug(("\t"*depth) + name + ": Attribute " + n + " not found")
                        additionalProperties
                    }
                  case None =>
//                    schema.`type` match {
//                      case Some(jst) =>
//                        jst.value.equals(Obj)
//                      case None =>
//                        //logger.debug(("\t"*depth) + name + ": No properties Found")
//                        false
//                    }
                    false
                }
              }
              }.reduce(_ && _)


              if (!additionalProperties && !allAttributesPass) {
                m.map { case (n, t) => {
                  schema.properties match {
                    case Some(s) =>
                      s.value.get(n) match {
                        case Some(v) =>

                        case None =>
                          //issues.head.put(name,issues.head.getOrElse(name,0)+1)
                          flattenAndAdd(t,issues.head,name+"."+n)
                      }
                    case None =>
                      flattenAndAdd(t,issues.head,name+"."+n)
                  }
                }
                }
              }

              if(!schema.`type`.get.value.equals(Obj)) {
                //issues.put("object_type_check", issues.getOrElse("object_type_check", 0) + 1)
              }
              schema.`type`.get.value.equals(Obj) &&
                (passesRequiredCheck || additionalProperties) &&
                allAttributesPass && passesMaxPropertiesCheck


            case JE_Array(a) =>
              val passesMaxItemsCheck = schema.maxItems match { // check on constraint
                case None => true
                case Some(max) =>
                  if (a.size > max.value) {
                    //logger.debug(("\t"*depth) + name + " maxItems: " + max.value.toString + " found size: " + a.size.toString)
                    if(strongCheck)
                      issues.head.put("array_max",issues.head.getOrElse("array_max",0)+1)
                    false
                  } else {
                    true
                  }
              }

              if (!schema.required.equals(None)) {
                //logger.error(("\t"*depth) + name + ": Unexpected required fields in array")
              }

              val arrayCheck: Boolean = (a.map(sArr => {
                schema.items match {
                  case Some(s) =>
                    validateRow(s.value, sArr,issues,false,depth+1, name + "[*]")
                  case None =>
                    schema.`type` match {
                      case Some(jst) => jst.value.equals(Arr)
                      case None =>
                        //logger.debug(("\t"*depth) + name + ": Empty Array Found")
                        false
                    }
                }
              }).reduce(_ && _))

              if(!schema.`type`.get.value.equals(Arr)) {
                //issues.put("array_type_check", issues.getOrElse("array_type_check", 0) + 1)
              }

              schema.`type`.get.value.equals(Arr) &&
                arrayCheck && passesMaxItemsCheck

          }
        }
    }

  }

  private def compareTypes(schema: JSS, attribute: Types.Json.JsonExplorerType): Boolean = {
    schema.`type` match {
      case Some(t) =>
        attribute match {
          case JE_String => return t.value.equals(Str)
          case JE_Numeric => return t.value.equals(Num)
          case JE_Boolean => return t.value.equals(Bool)
          case JE_Null => return t.value.equals(Null)
          case JE_Empty_Array => return t.value.equals(Arr)
          case JE_Empty_Object => return t.value.equals(Obj)
        }
      case None => // probably anyOf
        schema.anyOf match {
          case None =>
            schema.oneOf match {
              case None => throw new Exception("anyOf, oneOf, and type not found so something's probably not right") // idk what's going on, throw an error or something
              case Some(oneOf) => return oneOf.value.map(s => compareTypes(s,attribute)).reduce(_||_)
            }
          case Some(anyOf) => return anyOf.value.map(s => compareTypes(s,attribute)).reduce(_||_)
        }
    }

  }

  private def flattenAndAdd(t: JsonExplorerType, attributeMap: scala.collection.mutable.HashMap[String,Int], name: String): Unit = {
    t match {
      case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array | JE_Empty_Object =>
        attributeMap.put(name,attributeMap.getOrElse(name,0))

      case JE_Object(o) =>
        o.foreach(x =>
          flattenAndAdd(x._2,attributeMap,name+"."+x._1)
        )

      case JE_Array(a) =>
        a.foreach(x => flattenAndAdd(x,attributeMap,name+"[*]"))
    }

  }

}
