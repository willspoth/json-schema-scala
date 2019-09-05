import org.specs2.mutable.Specification


class BasicTests extends Specification {

  "Json-schema Parser" should {

    "Parse $id" >> {
      val v = JsonSchemaParser.jsFromString("{\n" +
        "\"$id\": \"https://example.com/person.schema.json\"" +
        "\n}")
      ok
    }

  }

}
