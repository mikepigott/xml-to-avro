/**
 * Copyright 2014 Mike Pigott
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mpigott.avro.xml;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.stream.StreamSource;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.junit.Test;

/**
 * Verifies the {@link AvroSchemaGenerator} generates the expected
 * {@link Schema} for <code>src/test/resources/test_schema.xsd</code>.
 *
 * @author  Mike Pigott
 */
public class TestAvroSchemaGenerator {

  @Test
  public void test() throws Exception {
    XmlSchemaCollection collection = null;
    FileReader fileReader = null;
    AvroSchemaGenerator visitor = new AvroSchemaGenerator();
    try {
      File file = new File("src\\test\\resources\\test_schema.xsd");
      fileReader = new FileReader(file);

      collection = new XmlSchemaCollection();
      collection.setSchemaResolver(new XmlSchemaMultiBaseUriResolver());
      collection.read(new StreamSource(fileReader, file.getAbsolutePath()));

    } finally {
      if (fileReader != null) {
        try {
          fileReader.close();
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }
    }

    XmlSchemaElement elem = getElementOf(collection, "root");
    XmlSchemaWalker walker = new XmlSchemaWalker(collection, visitor);
    walker.setUserRecognizedTypes( Utils.getAvroRecognizedTypes() );
    walker.walk(elem);

    Schema schema = visitor.getSchema();

    visitor.clear();
    walker.clear();

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals("org.apache.avro.AvroTest", schema.getNamespace());

    checkOptionalPrimitiveField(schema.getField("anySimpleType"),    Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("duration"),         Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("dateTime"),         Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("date"),             Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("time"),             Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("gYearMonth"),       Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("gYear"),            Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("gMonthDay"),        Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("gDay"),             Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("gMonth"),           Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("anyURI"),           Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("qname"),            Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("string"),           Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("normalizedString"), Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("token"),            Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("language"),         Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("nmtoken"),          Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("name"),             Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("ncName"),           Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("id"),               Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("idref"),            Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("entity"),           Schema.Type.STRING, null);
    checkOptionalPrimitiveField(schema.getField("nmtokens"),         Schema.Type.ARRAY,  Schema.Type.STRING);
    checkOptionalPrimitiveField(schema.getField("idrefs"),           Schema.Type.ARRAY,  Schema.Type.STRING);
    checkOptionalPrimitiveField(schema.getField("entities"),         Schema.Type.ARRAY,  Schema.Type.STRING);

    checkOptionalPrimitiveField(schema.getField("integer"),            Schema.Type.DOUBLE, null);
    checkOptionalPrimitiveField(schema.getField("nonPositiveInteger"), Schema.Type.DOUBLE, null);
    checkOptionalPrimitiveField(schema.getField("negativeInteger"),    Schema.Type.DOUBLE, null);
    checkOptionalPrimitiveField(schema.getField("long"),               Schema.Type.LONG,   null);
    checkOptionalPrimitiveField(schema.getField("int"),                Schema.Type.INT,    null);
    checkOptionalPrimitiveField(schema.getField("short"),              Schema.Type.INT,    null);
    checkOptionalPrimitiveField(schema.getField("byte"),               Schema.Type.INT,    null);
    checkOptionalPrimitiveField(schema.getField("nonNegativeInteger"), Schema.Type.DOUBLE, null);
    checkOptionalPrimitiveField(schema.getField("positiveInteger"),    Schema.Type.DOUBLE, null);
    checkOptionalPrimitiveField(schema.getField("unsignedLong"),       Schema.Type.DOUBLE, null);
    checkOptionalPrimitiveField(schema.getField("unsignedInt"),        Schema.Type.LONG,   null);
    checkOptionalPrimitiveField(schema.getField("unsignedShort"),      Schema.Type.INT,    null);
    checkOptionalPrimitiveField(schema.getField("unsignedByte"),       Schema.Type.INT,    null);

    List<Schema> rootSchemas = schema.getField("root").schema().getElementType().getTypes();
    assertNotNull(rootSchemas);

    Map<String, Schema> rootSchemasByName = new HashMap<String, Schema>();
    for (Schema rootSchema : rootSchemas) {
      rootSchemasByName.put(rootSchema.getName(), rootSchema);
    }

    checkPrimitiveField(rootSchemasByName.get("primitive").getField("primitive"),               Schema.Type.STRING, null);
    checkPrimitiveField(rootSchemasByName.get("nonNullPrimitive").getField("nonNullPrimitive"), Schema.Type.STRING, null);

    List<Schema> recordSchemas = rootSchemasByName.get("record").getField("record").schema().getElementType().getTypes();
    Map<String, Schema> recordSchemasByName = new HashMap<String, Schema>();
    for (Schema recordSchema : recordSchemas) {
      recordSchemasByName.put(recordSchema.getName(), recordSchema);
    }

    checkPrimitiveField(rootSchemasByName.get("map").getField("id"),    Type.STRING, null);
    checkPrimitiveField(rootSchemasByName.get("list").getField("size"), Type.DOUBLE, null);

    assertNotNull( rootSchemasByName.get("record").getField("record") );
    assertNotNull( rootSchemasByName.get("list").getField("list") );
    assertNotNull( rootSchemasByName.get("tuple").getField("tuple") );
    assertNotNull( rootSchemasByName.get("map").getField("map") );

    assertEquals(rootSchemasByName.get("primitive").getField("primitive"),               recordSchemasByName.get("primitive").getField("primitive"));
    assertEquals(rootSchemasByName.get("nonNullPrimitive").getField("nonNullPrimitive"), recordSchemasByName.get("nonNullPrimitive").getField("nonNullPrimitive"));
    assertEquals(rootSchemasByName.get("record").getField("record"),                     recordSchemasByName.get("record").getField("record"));
    assertEquals(rootSchemasByName.get("map").getField("map"),                           recordSchemasByName.get("map").getField("map"));
    assertEquals(rootSchemasByName.get("map").getField("id"),                            recordSchemasByName.get("map").getField("id"));
    assertEquals(rootSchemasByName.get("list").getField("list"),                         recordSchemasByName.get("list").getField("list"));
    assertEquals(rootSchemasByName.get("list").getField("size"),                         recordSchemasByName.get("list").getField("size"));
    assertEquals(rootSchemasByName.get("tuple").getField("tuple"),                       recordSchemasByName.get("tuple").getField("tuple"));

    List<Schema> listSchemas = rootSchemasByName.get("list").getField("list").schema().getElementType().getTypes();
    Map<String, Schema> listSchemasByName = new HashMap<String, Schema>();
    for (Schema listSchema : listSchemas) {
      listSchemasByName.put(listSchema.getName(), listSchema);
    }

    assertEquals(rootSchemasByName.get("primitive").getField("primitive"), listSchemasByName.get("primitive").getField("primitive"));
    assertEquals(rootSchemasByName.get("record").getField("record"),       listSchemasByName.get("record").getField("record"));
    assertEquals(rootSchemasByName.get("map").getField("map"),             listSchemasByName.get("map").getField("map"));
    assertEquals(rootSchemasByName.get("map").getField("id"),              listSchemasByName.get("map").getField("id"));

    List<Schema> mapSchemas = rootSchemasByName.get("map").getField("map").schema().getElementType().getTypes();
    Map<String, Schema> mapSchemasByName = new HashMap<String, Schema>();
    for (Schema mapSchema : mapSchemas) {
      mapSchemasByName.put(mapSchema.getName(), mapSchema);
    }

    assertEquals(rootSchemasByName.get("primitive").getField("primitive"),               mapSchemasByName.get("primitive").getField("primitive"));
    assertEquals(rootSchemasByName.get("nonNullPrimitive").getField("nonNullPrimitive"), mapSchemasByName.get("nonNullPrimitive").getField("nonNullPrimitive"));
    assertEquals(rootSchemasByName.get("record").getField("record"),                     mapSchemasByName.get("record").getField("record"));
    assertEquals(rootSchemasByName.get("map").getField("map"),                           mapSchemasByName.get("map").getField("map"));
    assertEquals(rootSchemasByName.get("map").getField("id"),                            mapSchemasByName.get("map").getField("id"));
    assertEquals(rootSchemasByName.get("list").getField("list"),                         mapSchemasByName.get("list").getField("list"));
    assertEquals(rootSchemasByName.get("list").getField("size"),                         mapSchemasByName.get("list").getField("size"));
    assertEquals(rootSchemasByName.get("tuple").getField("tuple"),                       mapSchemasByName.get("tuple").getField("tuple"));

    List<Schema> tupleSchemas = rootSchemasByName.get("tuple").getField("tuple").schema().getElementType().getTypes();
    Map<String, Schema> tupleSchemasByName = new HashMap<String, Schema>();
    for (Schema tupleSchema : tupleSchemas) {
      tupleSchemasByName.put(tupleSchema.getName(), tupleSchema);
    }

    assertEquals(rootSchemasByName.get("primitive").getField("primitive"),               tupleSchemasByName.get("primitive").getField("primitive"));
    assertEquals(rootSchemasByName.get("nonNullPrimitive").getField("nonNullPrimitive"), tupleSchemasByName.get("nonNullPrimitive").getField("nonNullPrimitive"));
    assertEquals(rootSchemasByName.get("record").getField("record"),                     tupleSchemasByName.get("record").getField("record"));
    assertEquals(rootSchemasByName.get("map").getField("map"),                           tupleSchemasByName.get("map").getField("map"));
    assertEquals(rootSchemasByName.get("map").getField("id"),                            tupleSchemasByName.get("map").getField("id"));
    assertEquals(rootSchemasByName.get("list").getField("list"),                         tupleSchemasByName.get("list").getField("list"));
    assertEquals(rootSchemasByName.get("list").getField("size"),                         tupleSchemasByName.get("list").getField("size"));

    // TODO: Confirm the XML-Schema-info encoded in JSON is also correct.
    // TODO: Also test default values.
  }

  private void checkPrimitiveField(Schema.Field field, Schema.Type type, Schema.Type subType) {
    assertNotNull(field);
    assertEquals(type, field.schema().getType());
    if (type.equals(Schema.Type.ARRAY)) {
      assertEquals(subType, field.schema().getElementType().getType());
    }
  }

  private void checkOptionalPrimitiveField(Schema.Field field, Schema.Type type, Schema.Type subType) {
    assertNotNull(field);
    assertEquals(field.schema().getType(), Schema.Type.UNION);
    assertEquals(2, field.schema().getTypes().size());

    boolean foundNullType = false;
    Schema realType = null;

    for (Schema schema : field.schema().getTypes()) {
      if (schema.getType().equals(Schema.Type.NULL)) {
        foundNullType = true;
      } else if (schema.getType().equals(type)) {
        realType = schema;
      }
    }

    assertTrue(foundNullType);
    assertNotNull(realType);

    if ( type.equals(Schema.Type.ARRAY) ) {
      assertEquals(subType, realType.getElementType().getType());
    }
  }

  private static XmlSchemaElement getElementOf(XmlSchemaCollection collection, String name) {
    XmlSchemaElement elem = null;
    for (XmlSchema schema : collection.getXmlSchemas()) {
      elem = schema.getElementByName(name);
      if (elem != null) {
        break;
      }
    }
    return elem;
  }
}
