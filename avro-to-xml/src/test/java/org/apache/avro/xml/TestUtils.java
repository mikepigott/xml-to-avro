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
package org.apache.avro.xml;

import static org.junit.Assert.*;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;
import javax.xml.namespace.QName;

import org.apache.avro.Schema;
import org.apache.avro.xml.Utils;
import org.apache.ws.commons.schema.constants.Constants;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.TextNode;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.Assert;

public class TestUtils {

  private static String NAMESPACE_URI =
      "http://www.sec.gov/Archives/edgar/data/1013237/000143774913004187/"
      + "fds-20130228.xsd";

  private static String EXPECTED_RESULT =
      "gov.sec.www.Archives.edgar.data.1013237.000143774913004187."
      + "fds_20130228.xsd";

  private static ArrayList<QName> avroUnrecognizedTypes;

  @Test
  public void testGetAvroNamespaceForString() throws URISyntaxException {
    Assert.assertEquals(
        EXPECTED_RESULT,
        Utils.getAvroNamespaceFor(NAMESPACE_URI));
  }

  @Test
  public void testGetAvroNamespaceForURL()
      throws MalformedURLException, URISyntaxException {
    Assert.assertEquals(
        EXPECTED_RESULT,
        Utils.getAvroNamespaceFor(new URL(NAMESPACE_URI)));
  }

  @Test
  public void testGetAvroNamespaceForURI() throws URISyntaxException {
    Assert.assertEquals(
        EXPECTED_RESULT,
        Utils.getAvroNamespaceFor(new URI(NAMESPACE_URI)));
  }

  @Test
  public void testUblUrn() throws URISyntaxException {
    URI uri =
        new URI("urn:oasis:names:specification:ubl:schema:xsd:"
                + "ApplicationResponse-2");
    
    Assert.assertEquals(
        "oasis.names.specification.ubl.schema.xsd.ApplicationResponse_2",
        Utils.getAvroNamespaceFor(uri));
  }

  @BeforeClass
  public static void setUpUnrecognizedTypes() {
    avroUnrecognizedTypes = new ArrayList<QName>();

    avroUnrecognizedTypes.add(Constants.XSD_ANY);
    avroUnrecognizedTypes.add(Constants.XSD_BYTE);
    avroUnrecognizedTypes.add(Constants.XSD_ENTITIES);
    avroUnrecognizedTypes.add(Constants.XSD_ENTITY);
    avroUnrecognizedTypes.add(Constants.XSD_IDREF);
    avroUnrecognizedTypes.add(Constants.XSD_IDREFS);
    avroUnrecognizedTypes.add(Constants.XSD_INTEGER);
    avroUnrecognizedTypes.add(Constants.XSD_LANGUAGE);
    avroUnrecognizedTypes.add(Constants.XSD_NAME);
    avroUnrecognizedTypes.add(Constants.XSD_NCNAME);
    avroUnrecognizedTypes.add(Constants.XSD_NEGATIVEINTEGER);
    avroUnrecognizedTypes.add(Constants.XSD_NMTOKEN);
    avroUnrecognizedTypes.add(Constants.XSD_NMTOKENS);
    avroUnrecognizedTypes.add(Constants.XSD_NONNEGATIVEINTEGER);
    avroUnrecognizedTypes.add(Constants.XSD_NONPOSITIVEINTEGER);
    avroUnrecognizedTypes.add(Constants.XSD_NORMALIZEDSTRING);
    avroUnrecognizedTypes.add(Constants.XSD_POSITIVEINTEGER);
    avroUnrecognizedTypes.add(Constants.XSD_SCHEMA);
    avroUnrecognizedTypes.add(Constants.XSD_TOKEN);
    avroUnrecognizedTypes.add(Constants.XSD_UNSIGNEDBYTE);
    avroUnrecognizedTypes.add(Constants.XSD_ANYSIMPLETYPE);
    avroUnrecognizedTypes.add(Constants.XSD_DURATION);
    avroUnrecognizedTypes.add(Constants.XSD_DATETIME);
    avroUnrecognizedTypes.add(Constants.XSD_TIME);
    avroUnrecognizedTypes.add(Constants.XSD_DATE);
    avroUnrecognizedTypes.add(Constants.XSD_YEARMONTH);
    avroUnrecognizedTypes.add(Constants.XSD_YEAR);
    avroUnrecognizedTypes.add(Constants.XSD_MONTHDAY);
    avroUnrecognizedTypes.add(Constants.XSD_DAY);
    avroUnrecognizedTypes.add(Constants.XSD_MONTH);
    avroUnrecognizedTypes.add(Constants.XSD_STRING);
    avroUnrecognizedTypes.add(Constants.XSD_ANYURI);
    avroUnrecognizedTypes.add(Constants.XSD_NOTATION);
  }

  @Test
  public void testGetAvroRecognizedTypes() {
    final Set<QName> recTypes = Utils.getAvroRecognizedTypes();

    assertTrue( recTypes.contains(Constants.XSD_ANYTYPE) );
    assertTrue( recTypes.contains(Constants.XSD_BOOLEAN) );
    assertTrue( recTypes.contains(Constants.XSD_DECIMAL) );
    assertTrue( recTypes.contains(Constants.XSD_DOUBLE) );
    assertTrue( recTypes.contains(Constants.XSD_FLOAT) );
    assertTrue( recTypes.contains(Constants.XSD_BASE64) );
    assertTrue( recTypes.contains(Constants.XSD_HEXBIN) );
    assertTrue( recTypes.contains(Constants.XSD_LONG) );
    assertTrue( recTypes.contains(Constants.XSD_ID) );
    assertTrue( recTypes.contains(Constants.XSD_INT) );
    assertTrue( recTypes.contains(Constants.XSD_UNSIGNEDINT) );
    assertTrue( recTypes.contains(Constants.XSD_UNSIGNEDSHORT) );
    assertTrue( recTypes.contains(Constants.XSD_UNSIGNEDLONG) );
    assertTrue( recTypes.contains(Constants.XSD_QNAME) );

    for (QName unrecognizedType : avroUnrecognizedTypes) {
      assertFalse( recTypes.contains(unrecognizedType) );
    }
  }

  @Test
  public void testGetAvroSchemaTypeForQName() {
    assertEquals(
        Schema.Type.STRING,
        Utils.getAvroSchemaTypeFor(Constants.XSD_ANYTYPE));

    assertEquals(
        Schema.Type.BOOLEAN,
        Utils.getAvroSchemaTypeFor(Constants.XSD_BOOLEAN));

    assertEquals(
        Schema.Type.DOUBLE,
        Utils.getAvroSchemaTypeFor(Constants.XSD_DECIMAL));

    assertEquals(
        Schema.Type.DOUBLE,
        Utils.getAvroSchemaTypeFor(Constants.XSD_DOUBLE));

    assertEquals(
        Schema.Type.FLOAT,
        Utils.getAvroSchemaTypeFor(Constants.XSD_FLOAT));

    assertEquals(
        Schema.Type.BYTES,
        Utils.getAvroSchemaTypeFor(Constants.XSD_BASE64));

    assertEquals(
        Schema.Type.BYTES,
        Utils.getAvroSchemaTypeFor(Constants.XSD_HEXBIN));

    assertEquals(
        Schema.Type.LONG,
        Utils.getAvroSchemaTypeFor(Constants.XSD_LONG));

    assertEquals(
        Schema.Type.STRING,
        Utils.getAvroSchemaTypeFor(Constants.XSD_ID));

    assertEquals(
        Schema.Type.INT,
        Utils.getAvroSchemaTypeFor(Constants.XSD_INT));

    assertEquals(
        Schema.Type.LONG,
        Utils.getAvroSchemaTypeFor(Constants.XSD_UNSIGNEDINT));

    assertEquals(
        Schema.Type.INT,
        Utils.getAvroSchemaTypeFor(Constants.XSD_UNSIGNEDSHORT));

    assertEquals(
        Schema.Type.DOUBLE,
        Utils.getAvroSchemaTypeFor(Constants.XSD_UNSIGNEDLONG));

    assertEquals(
        Schema.Type.RECORD,
        Utils.getAvroSchemaTypeFor(Constants.XSD_QNAME));

    for (QName unrecognizedType : avroUnrecognizedTypes) {
      assertNull( Utils.getAvroSchemaTypeFor(unrecognizedType) );
    }
  }

  @Test
  public void testCreateJsonNodeNoArgs() {
    assertNull( Utils.createJsonNodeFor(null, null) );
  }

  @Test
  public void testCreateJsonNodeBoolean() {
    final Schema schema = Schema.create(Schema.Type.BOOLEAN);

    JsonNode trueBoolNode = Utils.createJsonNodeFor("true", schema);

    assertTrue(trueBoolNode instanceof BooleanNode);
    assertTrue(((BooleanNode) trueBoolNode).asBoolean());

    JsonNode falseBoolNode = Utils.createJsonNodeFor("false", schema);
    assertTrue(falseBoolNode instanceof BooleanNode);
    assertFalse(((BooleanNode) falseBoolNode).asBoolean());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateInvalidJsonNodeBoolean() {
    final Schema schema = Schema.create(Schema.Type.BOOLEAN);
    Utils.createJsonNodeFor("fail!", schema);
  }

  @Test
  public void testCreateJsonNodeBytes() {
    byte[] bytes = new byte[] { 12, 127, -128, -2, 0 };
    Schema schema = Schema.create(Schema.Type.BYTES);

    // BASE64
    String bytesAsString = DatatypeConverter.printBase64Binary(bytes);
    JsonNode binaryNode = Utils.createJsonNodeFor(bytesAsString, schema);

    assertTrue(binaryNode instanceof TextNode);
    assertEquals(bytesAsString, binaryNode.asText());

    // HEX
    bytesAsString = DatatypeConverter.printHexBinary(bytes);
    binaryNode = Utils.createJsonNodeFor(bytesAsString, schema);

    assertTrue(binaryNode instanceof TextNode);
    assertEquals(bytesAsString, binaryNode.asText());
  }

  @Test
  public void testCreateJsonNodeString() {
    String value = "avro string";
    Schema schema = Schema.create(Schema.Type.STRING);

    JsonNode stringNode = Utils.createJsonNodeFor(value, schema);

    assertTrue(stringNode instanceof TextNode);
    assertEquals(value, stringNode.asText());
  }

  @Test
  public void testCreateJsonNodeDouble() {
    double number = 12807.217;
    Schema schema = Schema.create(Schema.Type.DOUBLE);

    JsonNode numberNode =
        Utils.createJsonNodeFor(Double.toString(number), schema);

    assertTrue(numberNode instanceof NumericNode);
    assertEquals(number, numberNode.asDouble(), 0.001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateInvalidJsonNodeDouble() {
    Schema schema = Schema.create(Schema.Type.DOUBLE);
    Utils.createJsonNodeFor("fail!", schema);
  }

  @Test
  public void testCreateJsonNodeFloat() {
    float number = 12807.217f;
    Schema schema = Schema.create(Schema.Type.FLOAT);

    JsonNode numberNode =
        Utils.createJsonNodeFor(Float.toString(number), schema);

    assertTrue(numberNode instanceof NumericNode);
    assertEquals(number, numberNode.asDouble(), 0.001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateInvalidJsonNodeFloat() {
    Schema schema = Schema.create(Schema.Type.FLOAT);
    Utils.createJsonNodeFor("fail!", schema);
  }

  @Test
  public void testCreateJsonNodeInt() {
    int number = 820107;
    Schema schema = Schema.create(Schema.Type.INT);

    JsonNode numberNode =
        Utils.createJsonNodeFor(Integer.toString(number), schema);

    assertTrue(numberNode instanceof NumericNode);
    assertEquals(number, numberNode.asInt());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateInvalidJsonNodeInt() {
    Schema schema = Schema.create(Schema.Type.INT);
    Utils.createJsonNodeFor("fail!", schema);
  }

  @Test
  public void testCreateJsonNodeLong() {
    long number = 12319891255687L;
    Schema schema = Schema.create(Schema.Type.LONG);

    JsonNode numberNode =
        Utils.createJsonNodeFor(Long.toString(number), schema);

    assertTrue(numberNode instanceof NumericNode);
    assertEquals(number, numberNode.asLong());

  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateInvalidJsonNodeLong() {
    Schema schema = Schema.create(Schema.Type.LONG);
    Utils.createJsonNodeFor("fail!", schema);
  }

  @Test
  public void testUnionWithoutStringOrBytes() {
    ArrayList<Schema> unionTypes = new ArrayList<Schema>(5);
    unionTypes.add( Schema.create(Schema.Type.INT) );
    unionTypes.add( Schema.create(Schema.Type.FLOAT) );
    unionTypes.add( Schema.create(Schema.Type.DOUBLE) );
    unionTypes.add( Schema.create(Schema.Type.BOOLEAN) );
    unionTypes.add( Schema.create(Schema.Type.LONG) );

    Schema schema = Schema.createUnion(unionTypes);

    checkBaseTypes(schema);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnionWithoutStringOrBytes() {
    ArrayList<Schema> unionTypes = new ArrayList<Schema>(5);
    unionTypes.add( Schema.create(Schema.Type.INT) );
    unionTypes.add( Schema.create(Schema.Type.FLOAT) );
    unionTypes.add( Schema.create(Schema.Type.DOUBLE) );
    unionTypes.add( Schema.create(Schema.Type.BOOLEAN) );
    unionTypes.add( Schema.create(Schema.Type.LONG) );

    Schema schema = Schema.createUnion(unionTypes);
    Utils.createJsonNodeFor("fail!", schema);
  }

  @Test
  public void testUnionWithString() {
    ArrayList<Schema> unionTypes = new ArrayList<Schema>(6);
    unionTypes.add( Schema.create(Schema.Type.INT) );
    unionTypes.add( Schema.create(Schema.Type.FLOAT) );
    unionTypes.add( Schema.create(Schema.Type.DOUBLE) );
    unionTypes.add( Schema.create(Schema.Type.BOOLEAN) );
    unionTypes.add( Schema.create(Schema.Type.LONG) );
    unionTypes.add( Schema.create(Schema.Type.STRING) );

    Schema schema = Schema.createUnion(unionTypes);

    checkBaseTypes(schema);

    String value = "hello";
    JsonNode result = Utils.createJsonNodeFor(value, schema);

    assertTrue(result.getClass().getName(), result instanceof TextNode);
    assertEquals(value, result.asText());
  }

  @Test
  public void testUnionWithBytes() {
    ArrayList<Schema> unionTypes = new ArrayList<Schema>(6);
    unionTypes.add( Schema.create(Schema.Type.INT) );
    unionTypes.add( Schema.create(Schema.Type.FLOAT) );
    unionTypes.add( Schema.create(Schema.Type.DOUBLE) );
    unionTypes.add( Schema.create(Schema.Type.BOOLEAN) );
    unionTypes.add( Schema.create(Schema.Type.LONG) );
    unionTypes.add( Schema.create(Schema.Type.BYTES) );

    Schema schema = Schema.createUnion(unionTypes);

    checkBaseTypes(schema);

    byte[] bytes = new byte[] { -128, -64, -32, -16, -8, 7, 15, 31, 63, 127 };
    String value = DatatypeConverter.printBase64Binary(bytes);

    JsonNode result = Utils.createJsonNodeFor(value, schema);

    assertTrue(result.getClass().getName(), result instanceof TextNode);
    assertEquals(value, result.asText());
  }

  @Test
  public void testUnionWithBytesAndString() {
    ArrayList<Schema> unionTypes = new ArrayList<Schema>(7);
    unionTypes.add( Schema.create(Schema.Type.INT) );
    unionTypes.add( Schema.create(Schema.Type.FLOAT) );
    unionTypes.add( Schema.create(Schema.Type.DOUBLE) );
    unionTypes.add( Schema.create(Schema.Type.BOOLEAN) );
    unionTypes.add( Schema.create(Schema.Type.LONG) );
    unionTypes.add( Schema.create(Schema.Type.BYTES) );
    unionTypes.add( Schema.create(Schema.Type.STRING) );

    Schema schema = Schema.createUnion(unionTypes);

    checkBaseTypes(schema);

    // String
    String value = "hello";
    JsonNode result = Utils.createJsonNodeFor(value, schema);

    assertTrue(result.getClass().getName(), result instanceof TextNode);
    assertEquals(value, result.asText());

    // Bytes
    byte[] bytes = new byte[] { -128, -64, -32, -16, -8, 7, 15, 31, 63, 127 };
    value = DatatypeConverter.printBase64Binary(bytes);
    result = Utils.createJsonNodeFor(value, schema);

    assertTrue(result.getClass().getName(), result instanceof TextNode);
    assertEquals(value, result.asText());
  }

  private void checkBaseTypes(Schema schema) {
    // Boolean
    String value = "true";
    JsonNode result = Utils.createJsonNodeFor(value, schema);
    assertTrue(result instanceof BooleanNode);
    assertTrue( result.asBoolean() );

    value = "false";
    result = Utils.createJsonNodeFor(value, schema);
    assertTrue(result instanceof BooleanNode);
    assertFalse( result.asBoolean() );

    // Float
    value = "12345.67";
    result = Utils.createJsonNodeFor(value, schema);
    assertTrue(result instanceof NumericNode);
    assertEquals(12345.67, result.asDouble(), 0.01);

    // Double
    value = "1234567.8901";
    result = Utils.createJsonNodeFor(value, schema);
    assertTrue(result instanceof NumericNode);
    assertEquals(1234567.8901, result.asDouble(), 0.0001);

    // Long
    value = "12319891255687";
    result = Utils.createJsonNodeFor(value, schema);
    assertTrue(result instanceof NumericNode);
    assertEquals(12319891255687L, result.asLong());

    // Int
    value = "820107";
    result = Utils.createJsonNodeFor(value, schema);
    assertTrue(result instanceof NumericNode);
    assertEquals(820107, result.asInt());
  }

  @Test
  public void testArrayOfUnionTypes() {
    ArrayList<Schema> unionTypes = new ArrayList<Schema>(7);
    unionTypes.add( Schema.create(Schema.Type.INT) );
    unionTypes.add( Schema.create(Schema.Type.FLOAT) );
    unionTypes.add( Schema.create(Schema.Type.DOUBLE) );
    unionTypes.add( Schema.create(Schema.Type.BOOLEAN) );
    unionTypes.add( Schema.create(Schema.Type.LONG) );
    unionTypes.add( Schema.create(Schema.Type.BYTES) );
    unionTypes.add( Schema.create(Schema.Type.STRING) );

    Schema unionSchema = Schema.createUnion(unionTypes);

    Schema schema = Schema.createArray(unionSchema);

    String value = "true 127 12319891255687 12345.67 1234567.8901 false hello";

    JsonNode result = Utils.createJsonNodeFor(value, schema);

    assertTrue(result instanceof ArrayNode);
    Iterator<JsonNode> iter = result.getElements();

    // First Node: Boolean
    JsonNode next = iter.next();
    assertTrue(next instanceof BooleanNode);
    assertTrue( next.asBoolean() );

    // Second Node: Int
    next = iter.next();
    assertTrue(next instanceof NumericNode);
    assertEquals(127, next.asInt());

    // Third Node: Long
    next = iter.next();
    assertTrue(next instanceof NumericNode);
    assertEquals(12319891255687L, next.asLong());

    // Fourth Node: Float
    next = iter.next();
    assertTrue(next instanceof NumericNode);
    assertEquals(12345.67, next.asDouble(), 0.001);

    // Fifth Node: Double
    next = iter.next();
    assertTrue(next instanceof NumericNode);
    assertEquals(1234567.8901, next.asDouble(), 0.00001);

    // Sixth Node: Boolean
    next = iter.next();
    assertTrue(next instanceof BooleanNode);
    assertFalse( next.asBoolean() );

    // Seventh Node: String
    next = iter.next();
    assertTrue(next instanceof TextNode);
    assertEquals("hello", next.asText());

    // No more.
    assertFalse( iter.hasNext() );
  }

  @Test
  public void testArrayOfLongs() {
    String value = "12319891255687";
    long longVal = 12319891255687L;

    Schema schema = Schema.createArray( Schema.create(Schema.Type.LONG) );

    // 1-element array
    JsonNode result = Utils.createJsonNodeFor(value, schema);

    assertTrue(result instanceof ArrayNode);

    Iterator<JsonNode> iter = result.getElements();

    JsonNode next = iter.next();
    assertTrue(next instanceof NumericNode);
    assertEquals(longVal, next.asLong());
    assertFalse( iter.hasNext() );

    // 4-element array
    value = "12319891255687 12319891255687 12319891255687 12319891255687";

    result = Utils.createJsonNodeFor(value, schema);
    assertTrue(result instanceof ArrayNode);

    iter = result.getElements();
    int count = 0;
    while (iter.hasNext()) {
      ++count;
      next = iter.next();
      assertTrue(next instanceof NumericNode);
      assertEquals(longVal, next.asLong());
    }
    assertEquals(4, count);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidArrayOfLongs() {
    Schema schema = Schema.createArray( Schema.create(Schema.Type.LONG) );
    Utils.createJsonNodeFor("fail!", schema);
  }

  @Test
  public void testArrayOfBooleans() {
    String value = "true false false true";

    Schema schema = Schema.createArray( Schema.create(Schema.Type.BOOLEAN) );

    JsonNode result = Utils.createJsonNodeFor(value, schema);

    assertTrue(result instanceof ArrayNode);

    Iterator<JsonNode> iter = result.getElements();

    assertTrue(  iter.next().asBoolean() );
    assertFalse( iter.next().asBoolean() );
    assertFalse( iter.next().asBoolean() );
    assertTrue(  iter.next().asBoolean() );
    assertFalse( iter.hasNext() );

  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidArrayOfBooleans() {
    Schema schema = Schema.createArray( Schema.create(Schema.Type.BOOLEAN) );
    Utils.createJsonNodeFor("fail! fail! fail!", schema);
  }
}
