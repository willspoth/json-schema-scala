import scala.collection.mutable.ArrayBuffer

object JsonSchema {

  sealed trait JsonSchemaType

  case object Str extends JsonSchemaType {
    override def toString: String = "string"
  }
  case object Obj extends JsonSchemaType {
    override def toString: String = "object"
  }
  case object Arr extends JsonSchemaType {
    override def toString: String = "array"
  }
  case object Num extends JsonSchemaType {
    override def toString: String = "number"
  }
  case object Bool extends JsonSchemaType {
    override def toString: String = "boolean"
  }
  case object Null extends JsonSchemaType {
    override def toString: String = "null"
  }
  case object Empty extends JsonSchemaType {
    override def toString: String = "empty"
  }


  sealed trait JsonSchemaStructure extends Any

  case class JSA_definitions(value: java.lang.String) extends JsonSchemaStructure
  case class JSA_schema(value: java.lang.String) extends JsonSchemaStructure
  case class JSA_id(value: java.lang.String) extends JsonSchemaStructure
  case class JSA_ref(value: java.lang.String) extends JsonSchemaStructure
  case class JSA_type(value: JsonSchemaType) extends JsonSchemaStructure
  case class JSA_title(value: java.lang.String) extends JsonSchemaStructure
  case class JSA_required(value: Array[String]) extends JsonSchemaStructure
  case class JSA_properties(value: Seq[JsonSchemaProperty]) extends JsonSchemaStructure
  case class JSA_pair(key: java.lang.String, value: JsonSchemaObject) extends JsonSchemaStructure
  case class JSA_description(value: java.lang.String) extends JsonSchemaStructure


  case class JsonSchemaProperty(name: String,
                                `type`: Option[JsonSchemaType],
                                description: Option[String],
                                properties: Option[Seq[JsonSchemaProperty]],
                                required: Option[Array[String]])
  {
    override def toString: String = {
      s""""$name":""" + "{" +
        Seq(("type",`type`),
          ("description",description),
          ("properties",properties),
          ("required",required)
        ).map(x => printSome(x._1,x._2)).filterNot(x => x.equals("")).mkString(",") +
        "}"
    }
  }

  object JsonSchemaProperty {
    @Override
    def apply(name:String, vs: Seq[JsonSchemaStructure]): JsonSchemaProperty = {
      var id: Option[String] = None
      var schema: Option[String] = None
      var title: Option[String] = None
      var `type`: Option[JsonSchemaType] = None
      var properties: Option[Seq[JsonSchemaProperty]] = None
      var required: Option[Array[String]] = None
      var description: Option[String] = None

      vs.foreach( jss => {
        jss match {
          case v: JSA_id => id = Some(v.value)
          case v: JSA_schema => schema = Some(v.value)
          case v: JSA_title => title = Some(v.value)
          case v: JSA_type => `type` = Some(v.value)
          case v: JSA_properties => properties = Some(v.value)
          case v: JSA_required => required = Some(v.value)
          case v: JSA_description => description = Some(v.value)
        }
      })

      return new JsonSchemaProperty(name,`type`,description,properties,required)
    }
  }


  case class JsonSchemaObject(id: Option[String],
                        ref: Option[String],
                        schema: Option[String],
                        title: Option[String],
                        `type`: Option[JsonSchemaType],
                        description: Option[String],
                        properties: Option[Seq[JsonSchemaProperty]],
                        required: Option[Array[String]])
  {
    override def toString: String = {
      "{" +
      Seq(("$id",id),
        ("$ref",ref),
        ("$schema",schema),
        ("title",title),
        ("type",`type`),
        ("description",description),
        ("properties",properties),
        ("required",required)
      ).map(x => printSome(x._1,x._2)).filterNot(x => x.equals("")).mkString(",") +
      "}"
    }
  }

  object JsonSchemaObject {
    @Override
    def apply(vs: Seq[JsonSchemaStructure]): JsonSchemaObject = {
      var id: Option[String] = None
      var ref: Option[String] = None
      var schema: Option[String] = None
      var title: Option[String] = None
      var `type`: Option[JsonSchemaType] = None
      var required: Option[Array[String]] = None
      var description: Option[String] = None
      var properties: Option[Seq[JsonSchemaProperty]] = None
      vs.foreach( jss => {
        jss match {
          case v: JSA_id => id = Some(v.value)
          case v: JSA_ref => ref = Some(v.value)
          case v: JSA_schema => schema = Some(v.value)
          case v: JSA_title => title = Some(v.value)
          case v: JSA_type => `type` = Some(v.value)
          case v: JSA_properties => properties = Some(v.value)
          case v: JSA_required => required = Some(v.value)
          case v: JSA_description => description = Some(v.value)
        }
      })
      return new JsonSchemaObject(id,ref,schema,title,`type`,description,properties,required)
    }
  }

  private def printSome(header: String, s: Option[Any]): String = {
    s match {
      case None => return ""
      case Some(v) =>
        v match {
          case _: String => return s""""${header}":"${v.toString}""""
          case _: Double => return s""""${header}":${v.toString}"""
          case x: Array[String] => return  s""""${header}":[${x.map("\""+_.toString+"\"").mkString(",")}]"""
          case _: JsonSchemaType => return s""""${header}":"${v.toString}""""
          case x: Seq[JsonSchemaProperty] =>
            return s""""${header}":{${x.map(_.toString).mkString(",")}}"""
          case _ =>
            throw new Exception("Unhandled type in printSome: " + v.getClass.toString)
        }
    }
  }
}
