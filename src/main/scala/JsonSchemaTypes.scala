object JsonSchemaTypes {

  sealed trait JsonSchemaType

  case object Str extends JsonSchemaType
  case object Obj extends JsonSchemaType
  case object Arr extends JsonSchemaType
  case object Num extends JsonSchemaType
  case object Bool extends JsonSchemaType
  case object Ref extends JsonSchemaType


  sealed trait JsonSchemaStructure extends Any

  case class JSA_definitions(value: java.lang.String) extends JsonSchemaStructure
  case class JSA_schema(value: java.lang.String) extends JsonSchemaStructure
  case class JSA_id(value: java.lang.String) extends JsonSchemaStructure
  case class JSA_type(value: java.lang.String) extends JsonSchemaStructure
  case class JSA_title(value: java.lang.String) extends JsonSchemaStructure
  case class JSA_required(value: Array[String]) extends JsonSchemaStructure
  case class JSA_properties(value: Seq[JsonSchemaProperty]) extends JsonSchemaStructure
  case class JSA_pair(key: java.lang.String, value: JsonSchema) extends JsonSchemaStructure
  case class JSA_description(value: java.lang.String) extends JsonSchemaStructure


  case class JsonSchemaProperty(name: String, `type`: Option[String], description: Option[String], properties: Option[Seq[JsonSchemaProperty]], required: Option[Array[String]])
  object JsonSchemaProperty {
    @Override
    def apply(name:String, vs: Seq[JsonSchemaStructure]): JsonSchemaProperty = {
      var id: Option[String] = None
      var schema: Option[String] = None
      var title: Option[String] = None
      var `type`: Option[String] = None
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


  case class JsonSchema(id: Option[String], schema: Option[String], title: Option[String], `type`: Option[String], properties: Option[Seq[JsonSchemaProperty]], required: Option[Array[String]])

  object JsonSchema {
    @Override
    def apply(vs: Seq[JsonSchemaStructure]): JsonSchema = {
      var id: Option[String] = None
      var schema: Option[String] = None
      var title: Option[String] = None
      var `type`: Option[String] = None
      var required: Option[Array[String]] = None
      var description: Option[String] = None
      var properties: Option[Seq[JsonSchemaProperty]] = None
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
      return new JsonSchema(id,schema,title,`type`,properties,required)
    }
  }
}
