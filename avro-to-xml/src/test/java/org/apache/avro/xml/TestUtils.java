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

import static org.junit.Assert.*;

import java.math.MathContext;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Set;

import javax.xml.namespace.QName;

import org.apache.avro.Schema;
import org.apache.avro.xml.Utils;
import org.apache.ws.commons.schema.constants.Constants;
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
    avroUnrecognizedTypes.add(Constants.XSD_UNSIGNEDLONG);
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
        Schema.Type.BYTES,
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
        Schema.Type.RECORD,
        Utils.getAvroSchemaTypeFor(Constants.XSD_QNAME));

    for (QName unrecognizedType : avroUnrecognizedTypes) {
      assertNull( Utils.getAvroSchemaTypeFor(unrecognizedType) );
    }
  }

  @Test
  public void testCreateDecimalSchema() {
    XmlSchemaTypeInfo decimalType =
        new XmlSchemaTypeInfo(XmlSchemaBaseSimpleType.DECIMAL);
    decimalType.setUserRecognizedType(Constants.XSD_DECIMAL);

    Schema decimalSchema =
        Utils.getAvroSchemaFor(decimalType, Constants.XSD_DECIMAL, false);

    assertEquals(Schema.Type.BYTES, decimalSchema.getType());
    assertEquals("decimal", decimalSchema.getJsonProp("logicalType").asText());

    assertEquals(MathContext.DECIMAL128.getPrecision(),
                 decimalSchema.getJsonProp("precision").asInt());
  }
}
