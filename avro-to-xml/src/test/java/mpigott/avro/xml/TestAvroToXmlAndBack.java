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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.w3c.dom.Document;

/**
 * Tests converting an XML document to an Avro datum and back.
 */
public class TestAvroToXmlAndBack {

  private static DocumentBuilderFactory dbf;
  private static EncoderFactory avroEncoderFactory;
  private static DecoderFactory avroDecoderFactory;

  private DocumentBuilder docBuilder;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    dbf = DocumentBuilderFactory.newInstance();
    dbf.setNamespaceAware(true);

    avroEncoderFactory = EncoderFactory.get();
    avroDecoderFactory = DecoderFactory.get();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    docBuilder = dbf.newDocumentBuilder();
  }

  @Test
  public void testRoot() throws Exception {
    final QName root = new QName("http://avro.apache.org/AvroTest", "root");

    final File schemaFile =
        UtilsForTests.buildFile("src", "test", "resources", "test_schema.xsd");

    final File xmlFile =
        UtilsForTests.buildFile("src", "test", "resources", "test1_root.xml");

    final XmlDatumConfig config =
        new XmlDatumConfig(
            schemaFile,
            "http://avro.apache.org/AvroTest",
            root);

    runTest(config, xmlFile);
  }

  @Test
  public void testChildren() throws Exception {
    final QName root = new QName("http://avro.apache.org/AvroTest", "root");
    final File schemaFile =
        UtilsForTests.buildFile("src", "test", "resources", "test_schema.xsd");

    final File xmlFile =
        UtilsForTests.buildFile("src",
                                "test",
                                "resources",
                                "test2_children.xml");

    final XmlDatumConfig config =
        new XmlDatumConfig(
            schemaFile,
            "http://avro.apache.org/AvroTest",
            root);

    runTest(config, xmlFile);
  }

  @Test
  public void testGrandchildren() throws Exception {
    final QName root = new QName("http://avro.apache.org/AvroTest", "root");

    final File schemaFile =
        UtilsForTests.buildFile("src", "test", "resources", "test_schema.xsd");

    final File xmlFile =
        UtilsForTests.buildFile("src",
                                "test",
                                "resources",
                                "test3_grandchildren.xml");

    final XmlDatumConfig config =
        new XmlDatumConfig(
            schemaFile,
            "http://avro.apache.org/AvroTest",
            root);

    runTest(config, xmlFile);
  }

  @Test
  public void testComplex() throws Exception {
    final QName root = new QName("urn:avro:complex_schema", "root");
    final File complexSchemaFile =
        UtilsForTests.buildFile("src",
                                "test",
                                "resources",
                                "complex_schema.xsd");

    final File testSchemaFile =
        UtilsForTests.buildFile("src", "test", "resources", "test_schema.xsd");

    final File xmlFile = 
        UtilsForTests.buildFile("src",
                                "test",
                                "resources",
                                "complex_test1.xml");

    final XmlDatumConfig config =
        new XmlDatumConfig(complexSchemaFile, "urn:avro:complex_schema", root);
    config.addSchemaFile(testSchemaFile);

    final Document xmlDoc = docBuilder.parse(xmlFile);

    final Document outDoc = convertToAvroAndBack(config, xmlDoc);

    final File expectedXml =
        UtilsForTests.buildFile("src",
                                "test",
                                "resources",
                                "complex_test1_out.xml");

    final Document expectedDoc = docBuilder.parse(expectedXml);

    UtilsForTests.assertEquivalent(expectedDoc, outDoc);
  }

  private Document convertToAvroAndBack(
      XmlDatumConfig config,
      Document xmlDoc) throws Exception {

    final XmlDatumWriter writer = new XmlDatumWriter(config);
    final Schema xmlToAvroSchema = writer.getSchema();

    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    final JsonEncoder encoder =
        avroEncoderFactory.jsonEncoder(xmlToAvroSchema, outStream, true);

    writer.write(xmlDoc, encoder);

    encoder.flush();

    final ByteArrayInputStream inStream =
        new ByteArrayInputStream( outStream.toByteArray() );

    final JsonDecoder decoder =
        avroDecoderFactory.jsonDecoder(xmlToAvroSchema, inStream);

    final XmlDatumReader reader = new XmlDatumReader();
    reader.setSchema(xmlToAvroSchema);

    return reader.read(null, decoder);
  }

  private void runTest(XmlDatumConfig config, File xmlFile) throws Exception {

    final Document xmlDoc = docBuilder.parse(xmlFile);

    final Document outDoc = convertToAvroAndBack(config, xmlDoc);

    UtilsForTests.assertEquivalent(xmlDoc, outDoc);
  }
}
