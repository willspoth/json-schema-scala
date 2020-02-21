package Metrics

import Types.Json.{JE_Array, JE_Boolean, JE_Empty_Array, JE_Empty_Object, JE_Null, JE_Numeric, JE_Object, JE_String}
import Types.JsonSchema.{Arr, Bool, JSA_additionalProperties, JSA_required, JSS, Null, Num, Obj, Str}
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer


object Validation {

  val logger = Logger.getLogger(this.getClass)

  def calculateValidation(schema: JSS, rows: Array[String]): (Double,Double) = {
    val res: Double = rows.map(x => {
      if(validateRow(schema,Types.Json.shred(x)))
        1.0
      else
        0.0
    }).reduce(_+_)

    (res,rows.size.toDouble)
  }

  def calculateValidation(schema: JSS, rows: RDD[String], limit: Int = 0): (Double,Double) = {
    val res: Double = rows.zipWithIndex().filter(x => (limit == 0 || x._2.toInt < limit)).map(_._1).map(x => {
      if(validateRow(schema,Types.Json.shred(x)))
        1.0
      else
        0.0
    }).reduce(_+_)

    (res,if(limit == 0) rows.count().toDouble else limit.toDouble)

  }

  def validateRow(schema: JSS, attribute: Types.Json.JsonExplorerType,depth: Int = 0,name: String = ""): Boolean = {
    logger.trace(("\t"*depth) + name + ": validating attribute")

    schema.anyOf match {
      case Some(anyOf) =>
        !anyOf.value.zipWithIndex.map( s => validateRow(s._1,attribute,depth+1,name+"anyOf["+s._2.toString+"]")).forall(!_)
      case None =>
        if (!schema.oneOf.equals(None)){
          val matches = schema.oneOf.get.value.map( s => validateRow(s,attribute,depth+1,name)).map(x => if (x) 1 else 0)
            .reduce(_+_)
          if (matches > 1) {
            logger.trace(("\t"*depth) + name + ": oneOf " + matches.toString + " matches found, expected 1")
          }
          matches >= 1 // oneof only used for types now, little hack
        } else {

          attribute match {
            case JE_String | JE_Numeric | JE_Boolean | JE_Null | JE_Empty_Array =>
              val typeCheck = compareTypes(schema, attribute)
              if (typeCheck) {
                logger.trace(("\t"*depth) + name + ": " + attribute.getClass + " found and ok")
              } else {
                logger.debug(("\t"*depth) + name + ": " + attribute.getClass + " found : " + schema.`type`.getOrElse(None).toString)
              }
              typeCheck

            case JE_Empty_Object =>
              val typeCheck = compareTypes(schema, attribute)
              if (typeCheck) {
                logger.trace(("\t"*depth) + name + ": " + attribute.getClass + " found and ok")
              } else {
                logger.debug(("\t"*depth) + name + ": " + attribute.getClass + " found : " + schema.`type`.getOrElse(None).toString)
              }

              typeCheck && (schema.additionalProperties.getOrElse(JSA_additionalProperties(false)).value || schema.required.getOrElse(JSA_required(Set[String]())).value.isEmpty)

            case JE_Object(m) =>
              schema.maxProperties match { // check on constraint
                case None =>
                case Some(max) =>
                  if (m.size > max.value) {
                    logger.debug(("\t"*depth) + name + " maxProperties: " + max.value.toString + " found size: " + m.size.toString)
                    return false
                  }
              }

              val passesRequiredCheck: Boolean = schema.required match {
                case Some(req) =>
                  if ((req.value.size > 0)) req.value.map(t => {
                    if (!m.contains(t)) {
                      logger.debug(("\t"*depth) + name + " does not contain required attribute: " + t)
                    }
                    m.contains(t)
                  }).reduce(_ && _)
                  else true
                case None => true
              }

              if (passesRequiredCheck == false) {
                logger.debug(("\t"*depth) + name + ": Failed required check")
              }

              val allAttributesPass: Boolean = m.map { case (n, t) => {
                schema.properties match {
                  case Some(s) =>
                    s.value.get(n) match {
                      case Some(v) =>
                        validateRow(v, t,depth+1, n)
                      case None =>
                        logger.debug(("\t"*depth) + name + ": Attribute " + n + " not found")
                        false
                    }
                  case None =>
                    schema.`type` match {
                      case Some(jst) =>
                        jst.value.equals(Obj)
                      case None =>
                        logger.debug(("\t"*depth) + name + ": No properties Found")
                        false
                    }
                }
              }
              }.reduce(_ && _)

              val additionalProperties: Boolean = schema.additionalProperties match {
                case Some(b) => b.value
                case None => false
              }

              if (additionalProperties && !passesRequiredCheck) {
                logger.debug(("\t"*depth) + name + "Passes only due to additional properties")
              }

              schema.`type`.get.value.equals(Obj) &&
                (passesRequiredCheck || additionalProperties) &&
                allAttributesPass


            case JE_Array(a) =>
              schema.maxItems match { // check on constraint
                case None =>
                case Some(max) =>
                  if (a.size > max.value) {
                    logger.debug(("\t"*depth) + name + " maxItems: " + max.value.toString + " found size: " + a.size.toString)
                    return false
                  }
              }

              if (!schema.required.equals(None)) {
                logger.error(("\t"*depth) + name + ": Unexpected required fields in array")
              }

              val arrayCheck: Boolean = (a.map(sArr => {
                schema.items match {
                  case Some(s) =>
                    validateRow(s.value, sArr,depth+1, name + "[*]")
                  case None =>
                    schema.`type` match {
                      case Some(jst) => jst.value.equals(Arr)
                      case None =>
                        logger.debug(("\t"*depth) + name + ": Empty Array Found")
                        false
                    }
                }
              }).reduce(_ && _))

              schema.`type`.get.value.equals(Arr) &&
                arrayCheck

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

}
