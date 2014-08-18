/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.xml;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.transform.stream.StreamSource;

import org.apache.avro.Schema;
import org.apache.avro.xml.AvroSchemaGenerator;
import org.apache.avro.xml.Utils;
import org.apache.avro.xml.XmlSchemaMultiBaseUriResolver;
import org.apache.avro.xml.XmlSchemaWalker;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.junit.Test;

/**
 * Verifies the {@link AvroSchemaGenerator} generates the expected
 * {@link Schema} for <code>src/test/resources/test_schema.xsd</code>.
 */
public class TestAvroSchemaGenerator {

  @Test
  public void testSchema() throws Exception {
    File file =
        UtilsForTests.buildFile("src", "test", "resources", "test_schema.xsd");
    ArrayList<File> schemaFiles = new ArrayList<File>(1);
    schemaFiles.add(file);

    Schema schema = createSchemaOf(file, "root");

    UtilsForTests.assertEquivalent(getExpectedTestSchema(), schema);
  }

  @Test
  public void testComplexSchema() throws Exception {
    File file =
        UtilsForTests.buildFile(
            "src",
            "test",
            "resources",
            "complex_schema.xsd");

    Schema schema = createSchemaOf(file, "root");

    FileWriter writer = new FileWriter("complex_schema.avsc");
    writer.write( schema.toString(true) );
    writer.close();

    UtilsForTests.assertEquivalent(getExpectedComplexSchema(), schema);
  }

  private static Schema createSchemaOf(File file, String rootName) throws Exception {
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

    XmlSchemaElement elem = getElementOf(collection, rootName);
    XmlSchemaWalker walker = new XmlSchemaWalker(collection, visitor);
    walker.setUserRecognizedTypes( Utils.getAvroRecognizedTypes() );
    walker.walk(elem);

    Schema schema = visitor.getSchema();

    visitor.clear();
    walker.clear();

    return schema;
  }

  private static XmlSchemaElement getElementOf(
      XmlSchemaCollection collection,
      String name) {

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

    Schema optionalStringSchema = getOptionalStringSchema();

    List<Schema> optionalStringArrayTypes = new ArrayList<Schema>(2);
    optionalStringArrayTypes.add(
        Schema.createArray(Schema.create(Schema.Type.STRING)));
    optionalStringArrayTypes.add( Schema.create(Schema.Type.NULL) );
    Schema optionalStringArraySchema =
        Schema.createUnion(optionalStringArrayTypes);

    Schema optionalDoubleSchema = getOptionalDoubleSchema();

    List<Schema> optionalLongTypes = new ArrayList<Schema>(2);
    optionalLongTypes.add( Schema.create(Schema.Type.LONG) );
    optionalLongTypes.add( Schema.create(Schema.Type.NULL) );
    Schema optionalLongSchema = Schema.createUnion(optionalLongTypes);

    Schema optionalIntSchema = getOptionalIntSchema();

    Schema optionalBooleanSchema = getOptionalBooleanSchema();

    Schema optionalBinarySchema = getOptionalBinarySchema();

    List<Schema> optionalFloatTypes = new ArrayList<Schema>(2);
    optionalFloatTypes.add( Schema.create(Schema.Type.FLOAT) );
    optionalFloatTypes.add( Schema.create(Schema.Type.NULL) );
    Schema optionalFloatSchema = Schema.createUnion(optionalFloatTypes);

    List<Schema> optionalQNameTypes = new ArrayList<Schema>(2);
    optionalQNameTypes.add( getQNameSchema() );
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
        new Schema.Field("integer", optionalBinarySchema, null, null));
    rootFields.add(
        new Schema.Field("negativeInteger", optionalBinarySchema, null, null));
    rootFields.add(
        new Schema.Field("positiveInteger", optionalBinarySchema, null, null));
    rootFields.add(
        new Schema.Field("unsignedLong", optionalBinarySchema, null, null));
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
        new Schema.Field("decimal", optionalBinarySchema, null, null));
    rootFields.add(
        new Schema.Field("float", optionalFloatSchema, null, null));
    rootFields.add(
        new Schema.Field("double", optionalDoubleSchema, null, null));

    rootFields.add(
        new Schema.Field(
            "normalizedString",
            optionalStringSchema,
            null,
            null) );

    rootFields.add(
        new Schema.Field(
            "nonPositiveInteger",
            optionalBinarySchema,
            null,
            null));

    rootFields.add(
        new Schema.Field(
            "nonNegativeInteger",
            optionalBinarySchema,
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
        new Schema.Field("size", optionalBinarySchema, null, null));

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
        new Schema.Field(
            "mapId",
            Schema.create(Schema.Type.STRING),
            null,
            null));

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

  private static Schema getExpectedComplexSchema() {
    final String namespace = "avro.complex_schema";

    Schema firstMapValue =
        Schema.createRecord("value", null, namespace, false);

    // simpleRestriction
    List<Schema.Field> simpleRestrictionFields = new ArrayList<Schema.Field>();
    simpleRestrictionFields.add(
        new Schema.Field(
            "default",
            Schema.create(Schema.Type.STRING),
            null,
            null));

    simpleRestrictionFields.add(
        new Schema.Field(
            "fixed",
            Schema.create(Schema.Type.INT),
            null,
            null));

    List<Schema> simpleRestrictionTypes = new ArrayList<Schema>(2);
    simpleRestrictionTypes.add( Schema.create(Schema.Type.BOOLEAN) );
    simpleRestrictionTypes.add( Schema.create(Schema.Type.BYTES) );

    simpleRestrictionFields.add(
        new Schema.Field(
            "simpleRestriction",
            Schema.createUnion(simpleRestrictionTypes),
            "Simple type null",
            null));

    Schema simpleRestrictionSchema =
        Schema.createRecord("simpleRestriction", null, namespace, false);
    simpleRestrictionSchema.setFields(simpleRestrictionFields);

    // simpleExtension
    List<Schema.Field> simpleExtensionFields = new ArrayList<Schema.Field>();
    simpleExtensionFields.add(
        new Schema.Field(
            "default",
            Schema.create(Schema.Type.STRING),
            null,
            null));

    simpleExtensionFields.add(
        new Schema.Field(
            "fixed",
            Schema.create(Schema.Type.INT),
            null,
            null));

    List<Schema> simpleExtensionTypes = new ArrayList<Schema>();
    simpleExtensionTypes.add( Schema.create(Schema.Type.BOOLEAN) );
    simpleExtensionTypes.add( Schema.create(Schema.Type.BYTES) );

    simpleExtensionFields.add(
        new Schema.Field(
            "simpleExtension",
            Schema.createUnion(simpleExtensionTypes),
            "Simple type null",
            null));

    Schema simpleExtensionSchema =
        Schema.createRecord("simpleExtension", null, namespace, false);
    simpleExtensionSchema.setFields(simpleExtensionFields);

    // anyAndFriends
    List<Schema.Field> anyAndFriendsFields = new ArrayList<Schema.Field>();
    anyAndFriendsFields.add(
        new Schema.Field(
            "anyAndFriends",
            Schema.create(Schema.Type.STRING),
            "Simple type null",
            null));

    Schema anyAndFriendsSchema =
        Schema.createRecord("anyAndFriends", null, namespace, false);
    anyAndFriendsSchema.setFields(anyAndFriendsFields);

    // prohibit and children
    List<Schema.Field> fixedSchemaFields = new ArrayList<Schema.Field>();
    fixedSchemaFields.add(
        new Schema.Field(
            "fixed",
            Schema.create(Schema.Type.BYTES),
            "Simple type {http://www.w3.org/2001/XMLSchema}decimal",
            null));

    Schema fixedSchema = Schema.createRecord("fixed", null, namespace, false);
    fixedSchema.setFields(fixedSchemaFields);

    List<Schema.Field> prohibitFields = new ArrayList<Schema.Field>();
    prohibitFields.add(
        new Schema.Field(
            "prohibit",
            Schema.createArray(
                Schema.createUnion(
                    Collections.singletonList(fixedSchema))),
            "Children of {urn:avro:complex_schema}prohibit",
            null));

    Schema prohibitSchema =
        Schema.createRecord("prohibit", null, namespace, false);
    prohibitSchema.setFields(prohibitFields);

    // allTheThings and children
    List<Schema.Field> firstMapValueFields = new ArrayList<Schema.Field>();
    firstMapValueFields.add(
        new Schema.Field(
            "value",
            getOptionalBinarySchema(),
            "Simple type {http://www.w3.org/2001/XMLSchema}decimal",
            null));

        firstMapValue.setFields(firstMapValueFields);

    List<Schema> firstMapChildren = new ArrayList<Schema>();
    firstMapChildren.add(firstMapValue);

    List<Schema.Field> firstMapFields = new ArrayList<Schema.Field>();
    firstMapFields.add(
        new Schema.Field(
            "firstMap",
            Schema.createArray(Schema.createUnion(firstMapChildren)),
            "Children of {urn:avro:complex_schema}firstMap",
            null));

    firstMapFields.add(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null));

    Schema firstMapRecord =
        Schema.createRecord(
            "firstMap",
            null,
            namespace,
            false);
    firstMapRecord.setFields(firstMapFields);

    List<Schema.Field> secondMapFields = new ArrayList<Schema.Field>();
    secondMapFields.add(
        new Schema.Field("id", Schema.create(Schema.Type.STRING), null, null));
    secondMapFields.add(
        new Schema.Field(
            "value",
            Schema.create(Schema.Type.STRING),
            null,
            null));
    secondMapFields.add(
        new Schema.Field(
            "secondMap",
            Schema.create(Schema.Type.NULL),
            "This element contains no attributes and no children.",
            null));

    Schema secondMapRecord =
        Schema.createRecord("secondMap", null, namespace, false);
    secondMapRecord.setFields(secondMapFields);

    List<Schema> unionOfMaps = new ArrayList<Schema>();
    unionOfMaps.add(firstMapRecord);
    unionOfMaps.add(secondMapRecord);

    Schema mapUnionOfMaps =
        Schema.createMap( Schema.createUnion(unionOfMaps) );

    List<Schema.Field> allTheThingsFields = new ArrayList<Schema.Field>();
    allTheThingsFields.add(
        new Schema.Field(
            "id",
            Schema.create(Schema.Type.STRING),
            null,
            null));

    allTheThingsFields.add(
        new Schema.Field(
            "truth",
            Schema.create(Schema.Type.BOOLEAN),
            null,
            null));

    List<Schema> listOfNumbersTypes = new ArrayList<Schema>(2);
    listOfNumbersTypes.add( Schema.create(Schema.Type.INT) );
    listOfNumbersTypes.add( Schema.create(Schema.Type.BYTES) );
    Schema listOfNumbersSchema =
        Schema.createArray(Schema.createUnion(listOfNumbersTypes));

    allTheThingsFields.add(
        new Schema.Field(
            "listOfNumbers",
            listOfNumbersSchema,
            null,
            null));

    allTheThingsFields.add(
        new Schema.Field(
            "allTheThings",
            Schema.createArray(
                Schema.createUnion(
                    Collections.singletonList(mapUnionOfMaps))),
            "Children of {urn:avro:complex_schema}allTheThings",
            null));

    Schema allTheThingsRecord =
        Schema.createRecord("allTheThings", null, namespace, false);
    allTheThingsRecord.setFields(allTheThingsFields);

    Schema allTheThingsSchema =
        Schema.createMap(allTheThingsRecord);

    // Backtrack and children
    List<String> avroEnumSymbols = new ArrayList<String>(6);
    avroEnumSymbols.add("avro");
    avroEnumSymbols.add("json");
    avroEnumSymbols.add("xml");
    avroEnumSymbols.add("thrift");
    avroEnumSymbols.add("rest_li");
    avroEnumSymbols.add("protobuf");

    Schema avroEnumSchema =
        Schema.createEnum(
            "avroEnum",
            "Enumeration of symbols in {urn:avro:complex_schema}avroEnum",
            namespace + ".enums",
            avroEnumSymbols);

    Schema avroEnumRecord =
        Schema.createRecord(
            "avroEnum",
            null,
            namespace,
            false);

    ArrayList<Schema.Field> avroEnumFields = new ArrayList<Schema.Field>();
    avroEnumFields.add(
        new Schema.Field(
            "avroEnum",
            avroEnumSchema,
            "Simple type {http://www.w3.org/2001/XMLSchema}anyType",
            null));
    avroEnumRecord.setFields(avroEnumFields);

    Schema xmlEnumRecord =
        Schema.createRecord("xmlEnum", null, namespace, false);

    List<Schema.Field> xmlEnumFields = new ArrayList<Schema.Field>();
    xmlEnumFields.add(
        new Schema.Field(
            "xmlEnum",
            Schema.create(Schema.Type.STRING),
            "Simple type {http://www.w3.org/2001/XMLSchema}anyType",
            null));

    xmlEnumRecord.setFields(xmlEnumFields);

    Schema unsignedLongList =
        Schema.createRecord("unsignedLongList", null, namespace, false);

    List<Schema.Field> unsignedLongListFields = new ArrayList<Schema.Field>();
    unsignedLongListFields.add(
        new Schema.Field(
            "unsignedLongList",
            Schema.createArray(Schema.create(Schema.Type.BYTES)),
            "Simple type null",
            null));
    unsignedLongList.setFields(unsignedLongListFields);

    Schema listOfUnion =
        Schema.createRecord("listOfUnion", null, namespace, false);

    List<Schema> listOfUnionTypes = new ArrayList<Schema>();
    listOfUnionTypes.add( Schema.create(Schema.Type.BYTES) );
    listOfUnionTypes.add( Schema.create(Schema.Type.BOOLEAN) );
    listOfUnionTypes.add( Schema.create(Schema.Type.INT) );
    listOfUnionTypes.add( Schema.create(Schema.Type.STRING) );

    List<Schema.Field> listOfUnionFields = new ArrayList<Schema.Field>();
    listOfUnionFields.add(
        new Schema.Field(
            "listOfUnion",
            Schema.createArray( Schema.createUnion(listOfUnionTypes) ),
            "Simple type null",
            null));
    listOfUnion.setFields(listOfUnionFields);

    List<Schema.Field> qNameFields = new ArrayList<Schema.Field>();
    qNameFields.add(
        new Schema.Field(
            "qName",
            getQNameSchema(),
            "Simple type {http://www.w3.org/2001/XMLSchema}QName",
            null));
    Schema qNameSchema =
        Schema.createRecord("qName", null, namespace, false);
    qNameSchema.setFields(qNameFields);

    Schema backtrackSchema =
        Schema.createRecord(
            "backtrack",
            " This forces backtracking through the different schema options. "
            + "Consider the following elements: <backtrack> "
            + "<qName>avro:qName</qName> <avroEnum>avro</avroEnum> "
            + "<xmlEnum>rest.li</xmlEnum> <xmlEnum>xml</xmlEnum> "
            + "<unsignedLongList>18446744073709551615 1844674407370955 "
            + "12579</unsignedLongList> <listOfUnion>true 18446744073709551616"
            + " false -2147483648 -1234.567 avro</listOfUnion> </backtrack> "
            + "The first four elements in the list can match either the first "
            + "choice group or the second sequence group, and by default the "
            + "first branch will be taken. It is not until the last child "
            + "element, <listOfUnion>, is reached, that it becomes clear the "
            + "choice group should not be followed. ",
            namespace,
            false);

    List<Schema> backtrackChildren = new ArrayList<Schema>();
    backtrackChildren.add(qNameSchema);
    backtrackChildren.add(avroEnumRecord);
    backtrackChildren.add(xmlEnumRecord);
    backtrackChildren.add(unsignedLongList);
    backtrackChildren.add(listOfUnion);

    List<Schema.Field> backtrackFields = new ArrayList<Schema.Field>();
    backtrackFields.add(
        new Schema.Field(
            "backtrack",
            Schema.createArray(Schema.createUnion(backtrackChildren)),
            "Children of {urn:avro:complex_schema}backtrack",
            null) );

    backtrackSchema.setFields(backtrackFields);

    // complexExtension
    List<Schema.Field> complexExtensionFields = new ArrayList<Schema.Field>();

    complexExtensionFields.add(
        new Schema.Field("optional", getOptionalStringSchema(), null, null));

    complexExtensionFields.add(
        new Schema.Field(
            "defaulted",
            Schema.create(Schema.Type.STRING),
            null,
            null));

    List<Schema> complexExtensionChildren = new ArrayList<Schema>(3);
    complexExtensionChildren.add(unsignedLongList);
    complexExtensionChildren.add(listOfUnion);
    complexExtensionChildren.add(fixedSchema);

    complexExtensionFields.add(
        new Schema.Field(
            "complexExtension",
            Schema.createArray(Schema.createUnion(complexExtensionChildren)),
            "Children of {urn:avro:complex_schema}complexExtension",
            null));

    Schema complexExtensionSchema =
        Schema.createRecord("complexExtension", null, namespace, false);
    complexExtensionSchema.setFields(complexExtensionFields);

    // mixedType
    List<Schema> mixedTypeTypes = new ArrayList<Schema>();
    mixedTypeTypes.add( Schema.create(Schema.Type.STRING) );
    mixedTypeTypes.add(listOfUnion);
    mixedTypeTypes.add(unsignedLongList);

    List<Schema.Field> mixedTypeFields = new ArrayList<Schema.Field>();
    mixedTypeFields.add(
        new Schema.Field(
            "mixedType",
            Schema.createArray(Schema.createUnion(mixedTypeTypes)),
            "Children of {urn:avro:complex_schema}mixedType",
            null));

    Schema mixedTypeSchema =
        Schema.createRecord("mixedType", null, namespace, false);
    mixedTypeSchema.setFields(mixedTypeFields);

    // realRoot
    Schema realRootSchema =
        Schema.createRecord("realRoot", null, namespace, false);

    List<Schema.Field> realRootSchemaFields = new ArrayList<Schema.Field>();
    realRootSchemaFields.add(
        new Schema.Field(
            "month",
            Schema.create(Schema.Type.STRING),
            null,
            null));

    realRootSchemaFields.add(
        new Schema.Field(
          "year",
          Schema.create(Schema.Type.STRING),
          null,
          null));

    realRootSchemaFields.add(
        new Schema.Field(
            "day",
            Schema.create(Schema.Type.STRING),
            null,
            null));

    List<Schema> realRootChildren = new ArrayList<Schema>();
    realRootChildren.add(backtrackSchema);
    realRootChildren.add(allTheThingsSchema);
    realRootChildren.add(prohibitSchema);
    realRootChildren.add(anyAndFriendsSchema);
    realRootChildren.add(simpleExtensionSchema);
    realRootChildren.add(simpleRestrictionSchema);
    realRootChildren.add(complexExtensionSchema);
    realRootChildren.add(mixedTypeSchema);

    realRootSchemaFields.add(
        new Schema.Field(
            "realRoot",
            Schema.createArray(Schema.createUnion(realRootChildren)),
            "Children of {urn:avro:complex_schema}realRoot",
            null));

    realRootSchema.setFields(realRootSchemaFields);

    return Schema.createUnion( Collections.singletonList(realRootSchema));
  }

  private static Schema getOptionalStringSchema() {
    List<Schema> optionalStringTypes = new ArrayList<Schema>(2);
    optionalStringTypes.add( Schema.create(Schema.Type.STRING) );
    optionalStringTypes.add( Schema.create(Schema.Type.NULL) );
    return Schema.createUnion(optionalStringTypes);
  }

  private static Schema getQNameSchema() {
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

    return qNameSchema;
  }

  private static Schema getOptionalDoubleSchema() {
    List<Schema> optionalDoubleTypes = new ArrayList<Schema>(2);
    optionalDoubleTypes.add( Schema.create(Schema.Type.DOUBLE) );
    optionalDoubleTypes.add( Schema.create(Schema.Type.NULL) );
    return Schema.createUnion(optionalDoubleTypes);
  }

  private static Schema getOptionalBooleanSchema() {
    List<Schema> optionalBooleanTypes = new ArrayList<Schema>(2);
    optionalBooleanTypes.add( Schema.create(Schema.Type.BOOLEAN) );
    optionalBooleanTypes.add( Schema.create(Schema.Type.NULL) );
    return Schema.createUnion(optionalBooleanTypes);
  }

  private static Schema getOptionalIntSchema() {
    List<Schema> optionalIntTypes = new ArrayList<Schema>(2);
    optionalIntTypes.add( Schema.create(Schema.Type.INT) );
    optionalIntTypes.add( Schema.create(Schema.Type.NULL) );
    return Schema.createUnion(optionalIntTypes);
  }

  private static Schema getOptionalBinarySchema() {
    List<Schema> optionalBinaryTypes = new ArrayList<Schema>(2);
    optionalBinaryTypes.add( Schema.create(Schema.Type.BYTES) );
    optionalBinaryTypes.add( Schema.create(Schema.Type.NULL) );
    return Schema.createUnion(optionalBinaryTypes);
  }
}
