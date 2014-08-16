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
import java.util.ArrayList;
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
  public void testSchema() throws Exception {
    File file = new File("src\\test\\resources\\test_schema.xsd");
    ArrayList<File> schemaFiles = new ArrayList<File>(1);
    schemaFiles.add(file);

    XmlSchemaCollection collection = null;
    FileReader fileReader = null;
    AvroSchemaGenerator visitor =
        new AvroSchemaGenerator(null, null, schemaFiles);
    try {
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

    DocumentComparer.assertEquivalent(getExpectedTestSchema(), schema);
  }

  @Test
  public void testComplexSchema() throws Exception {
    File file = new File("src\\test\\resources\\complex_schema.xsd");
    ArrayList<File> schemaFiles = new ArrayList<File>(1);
    schemaFiles.add(file);

    XmlSchemaCollection collection = null;
    FileReader fileReader = null;
    AvroSchemaGenerator visitor =
        new AvroSchemaGenerator(null, null, schemaFiles);
    try {
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

  private static Schema getExpectedTestSchema() {
    final String namespace = "org.apache.avro.AvroTest";

    List<Schema> optionalStringTypes = new ArrayList<Schema>(2);
    optionalStringTypes.add( Schema.create(Schema.Type.STRING) );
    optionalStringTypes.add( Schema.create(Schema.Type.NULL) );
    Schema optionalStringSchema = Schema.createUnion(optionalStringTypes);

    List<Schema> optionalStringArrayTypes = new ArrayList<Schema>(2);
    optionalStringArrayTypes.add(
        Schema.createArray(Schema.create(Schema.Type.STRING)));
    optionalStringArrayTypes.add( Schema.create(Schema.Type.NULL) );
    Schema optionalStringArraySchema =
        Schema.createUnion(optionalStringArrayTypes);

    List<Schema> optionalDoubleTypes = new ArrayList<Schema>(2);
    optionalDoubleTypes.add( Schema.create(Schema.Type.DOUBLE) );
    optionalDoubleTypes.add( Schema.create(Schema.Type.NULL) );
    Schema optionalDoubleSchema = Schema.createUnion(optionalDoubleTypes);

    List<Schema> optionalLongTypes = new ArrayList<Schema>(2);
    optionalLongTypes.add( Schema.create(Schema.Type.LONG) );
    optionalLongTypes.add( Schema.create(Schema.Type.NULL) );
    Schema optionalLongSchema = Schema.createUnion(optionalLongTypes);

    List<Schema> optionalIntTypes = new ArrayList<Schema>(2);
    optionalIntTypes.add( Schema.create(Schema.Type.INT) );
    optionalIntTypes.add( Schema.create(Schema.Type.NULL) );
    Schema optionalIntSchema = Schema.createUnion(optionalIntTypes);

    List<Schema> optionalBooleanTypes = new ArrayList<Schema>(2);
    optionalBooleanTypes.add( Schema.create(Schema.Type.BOOLEAN) );
    optionalBooleanTypes.add( Schema.create(Schema.Type.NULL) );
    Schema optionalBooleanSchema = Schema.createUnion(optionalBooleanTypes);

    List<Schema> optionalBinaryTypes = new ArrayList<Schema>(2);
    optionalBinaryTypes.add( Schema.create(Schema.Type.BYTES) );
    optionalBinaryTypes.add( Schema.create(Schema.Type.NULL) );
    Schema optionalBinarySchema = Schema.createUnion(optionalBinaryTypes);

    List<Schema> optionalFloatTypes = new ArrayList<Schema>(2);
    optionalFloatTypes.add( Schema.create(Schema.Type.FLOAT) );
    optionalFloatTypes.add( Schema.create(Schema.Type.NULL) );
    Schema optionalFloatSchema = Schema.createUnion(optionalFloatTypes);

    Schema qNameSchema =
        Schema.createRecord(
            "qName",
            "Qualified Name",
            "org.w3.www.2001.XMLSchema",
            false);

    List<Schema.Field> qNameFields = new ArrayList<Schema.Field>(2);
    qNameFields.add(
        new Schema.Field(
            "namespace",
            Schema.create(Schema.Type.STRING),
            "The namespace of this qualified name.",
            null));
    qNameFields.add(
        new Schema.Field(
            "localPart",
            Schema.create(Schema.Type.STRING),
            "The local part of this qualified name.",
            null));
    qNameSchema.setFields(qNameFields);

    List<Schema> optionalQNameTypes = new ArrayList<Schema>(2);
    optionalQNameTypes.add(qNameSchema);
    optionalQNameTypes.add(Schema.create(Schema.Type.NULL));
    Schema optionalQNameSchema = Schema.createUnion(optionalQNameTypes);

    List<Schema.Field> rootFields = new ArrayList<Schema.Field>();
    rootFields.add(
        new Schema.Field("anySimpleType", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("duration", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("dateTime", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("date", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("time", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("gYearMonth", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("gYear", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("gMonthDay", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("gDay", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("gMonth", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("anyURI", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("qname", optionalQNameSchema, null, null) );
    rootFields.add(
        new Schema.Field("string", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("normalizedString", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("token", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("language", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("nmtoken", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("name", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("ncName", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("id", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("idref", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("entity", optionalStringSchema, null, null) );
    rootFields.add(
        new Schema.Field("nmtokens", optionalStringArraySchema, null, null));
    rootFields.add(
        new Schema.Field("idrefs", optionalStringArraySchema, null, null));
    rootFields.add(
        new Schema.Field("entities", optionalStringArraySchema, null, null));
    rootFields.add(
        new Schema.Field("integer", optionalDoubleSchema, null, null));
    rootFields.add(
        new Schema.Field("negativeInteger", optionalDoubleSchema, null, null));
    rootFields.add(
        new Schema.Field("positiveInteger", optionalDoubleSchema, null, null));
    rootFields.add(
        new Schema.Field("unsignedLong", optionalDoubleSchema, null, null));
    rootFields.add(
        new Schema.Field("long", optionalLongSchema, null, null));
    rootFields.add(
        new Schema.Field("unsignedInt", optionalLongSchema, null, null));
    rootFields.add(
        new Schema.Field("int", optionalIntSchema, null, null));
    rootFields.add(
        new Schema.Field("short", optionalIntSchema, null, null));
    rootFields.add(
        new Schema.Field("byte", optionalIntSchema, null, null));
    rootFields.add(
        new Schema.Field("unsignedShort", optionalIntSchema, null, null));
    rootFields.add(
        new Schema.Field("unsignedByte", optionalIntSchema, null, null));
    rootFields.add(
        new Schema.Field("boolean", optionalBooleanSchema, null, null));
    rootFields.add(
        new Schema.Field("base64Binary", optionalBinarySchema, null, null));
    rootFields.add(
        new Schema.Field("hexBinary", optionalBinarySchema, null, null));
    rootFields.add(
        new Schema.Field("decimal", optionalDoubleSchema, null, null));
    rootFields.add(
        new Schema.Field("float", optionalFloatSchema, null, null));
    rootFields.add(
        new Schema.Field("double", optionalDoubleSchema, null, null));

    rootFields.add(
        new Schema.Field(
            "nonPositiveInteger",
            optionalDoubleSchema,
            null,
            null));

    rootFields.add(
        new Schema.Field(
            "nonNegativeInteger",
            optionalDoubleSchema,
            null,
            null));

    ArrayList<String> nonNullPrimitiveEnumSymbols = new ArrayList<String>();
    nonNullPrimitiveEnumSymbols.add("boolean");
    nonNullPrimitiveEnumSymbols.add("int");
    nonNullPrimitiveEnumSymbols.add("long");
    nonNullPrimitiveEnumSymbols.add("float");
    nonNullPrimitiveEnumSymbols.add("double");
    nonNullPrimitiveEnumSymbols.add("decimal");
    nonNullPrimitiveEnumSymbols.add("bytes");
    nonNullPrimitiveEnumSymbols.add("string");

    Schema nonNullPrimitiveEnumSchema =
        Schema.createEnum(
            "nonNullPrimitive",
            "Enumeration of symbols in "
            + "{http://avro.apache.org/AvroTest}nonNullPrimitive",
            namespace + ".enums",
            nonNullPrimitiveEnumSymbols);

    Schema.Field nonNullPrimitiveEnumField =
        new Schema.Field(
            "nonNullPrimitive",
            nonNullPrimitiveEnumSchema,
            "Simple type {http://www.w3.org/2001/XMLSchema}anyType",
            null);

    Schema nonNullPrimitiveRecord =
        Schema.createRecord(
            "nonNullPrimitive",
            null,
            namespace,
            false);

    List<Schema.Field> nonNullPrimitiveFields = new ArrayList<Schema.Field>();
    nonNullPrimitiveFields.add(nonNullPrimitiveEnumField);
    nonNullPrimitiveRecord.setFields(nonNullPrimitiveFields);

    ArrayList<String> primitiveEnumSymbols =
        (ArrayList<String>) nonNullPrimitiveEnumSymbols.clone();
    primitiveEnumSymbols.add("null");

    Schema primitiveEnumSchema =
        Schema.createEnum(
            "primitive",
            "Enumeration of symbols in "
            + "{http://avro.apache.org/AvroTest}primitive",
            namespace + ".enums",
            primitiveEnumSymbols);

    Schema.Field primitiveEnumField =
        new Schema.Field(
            "primitive",
            primitiveEnumSchema,
            "Simple type {http://www.w3.org/2001/XMLSchema}anyType",
            null);

    Schema primitiveRecord =
        Schema.createRecord(
            "primitive",
            null,
            namespace,
            false);

    ArrayList<Schema.Field> primitiveFields = new ArrayList<Schema.Field>(1);
    primitiveFields.add(primitiveEnumField);
    primitiveRecord.setFields(primitiveFields);

    Schema recordSchema =
        Schema.createRecord("record", null, namespace, false);

    Schema.Field recordField =
        new Schema.Field("record", recordSchema, null, null);

    Schema mapRecordSchema =
        Schema.createRecord("map", null, namespace, false);

    Schema mapSchema =
        Schema.createMap(mapRecordSchema);

    Schema listSchema =
        Schema.createRecord("list", null, namespace, false);

    List<Schema> listChildren =
        new ArrayList<Schema>();
    listChildren.add(recordSchema);
    listChildren.add(mapSchema);
    listChildren.add(primitiveRecord);

    Schema listChildSchema =
        Schema.createArray(Schema.createUnion(listChildren));

    List<Schema.Field> listFields = new ArrayList<Schema.Field>();
    listFields.add(
        new Schema.Field(
            "list",
            listChildSchema,
            "Children of {http://avro.apache.org/AvroTest}list",
            null));
    listFields.add(
        new Schema.Field("size", optionalDoubleSchema, null, null));

    listSchema.setFields(listFields);

    Schema tupleSchema =
        Schema.createRecord("tuple", null, namespace, false);

    List<Schema> tupleChildren =
        new ArrayList<Schema>();
    tupleChildren.add(primitiveRecord);
    tupleChildren.add(nonNullPrimitiveRecord);
    tupleChildren.add(recordSchema);
    tupleChildren.add(mapSchema);
    tupleChildren.add(listSchema);

    Schema tupleChildSchema =
        Schema.createArray(Schema.createUnion(tupleChildren));

    List<Schema.Field> tupleChildrenFields =
        new ArrayList<Schema.Field>();

    tupleChildrenFields.add(
        new Schema.Field(
            "tuple",
            tupleChildSchema,
            "Children of {http://avro.apache.org/AvroTest}tuple",
            null));

    tupleSchema.setFields(tupleChildrenFields);

    List<Schema> recordChildSchemas = new ArrayList<Schema>();
    recordChildSchemas.add(primitiveRecord);
    recordChildSchemas.add(nonNullPrimitiveRecord);
    recordChildSchemas.add(recordSchema);
    recordChildSchemas.add(listSchema);
    recordChildSchemas.add(tupleSchema);
    recordChildSchemas.add(mapSchema);

    Schema recordChildSchema =
        Schema.createArray(Schema.createUnion(recordChildSchemas));

    ArrayList<Schema.Field> recordFields = new ArrayList<Schema.Field>(); 
    recordFields.add(
        new Schema.Field(
            "record",
            recordChildSchema,
            "Children of {http://avro.apache.org/AvroTest}record",
            null));

    recordSchema.setFields(recordFields);

    List<Schema> mapChildSchemas = new ArrayList<Schema>();
    mapChildSchemas.add(primitiveRecord);
    mapChildSchemas.add(nonNullPrimitiveRecord);
    mapChildSchemas.add(recordSchema);
    mapChildSchemas.add(listSchema);
    mapChildSchemas.add(tupleSchema);
    mapChildSchemas.add(mapSchema);

    Schema mapChildSchema =
        Schema.createArray(Schema.createUnion(mapChildSchemas));

    List<Schema.Field> mapFields = new ArrayList<Schema.Field>();
    mapFields.add(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null));
    mapFields.add(
        new Schema.Field(
            "map",
            mapChildSchema,
            "Children of {http://avro.apache.org/AvroTest}map",
            null));

    mapRecordSchema.setFields(mapFields);

    Schema rootChildSchema =
        Schema.createArray(Schema.createUnion(mapChildSchemas));

    rootFields.add(
        new Schema.Field(
            "root",
            rootChildSchema,
            "Children of {http://avro.apache.org/AvroTest}root",
            null) );

    Schema rootSchema = Schema.createRecord("root", null, namespace, false);
    rootSchema.setFields(rootFields);

    return rootSchema;
  }
}
