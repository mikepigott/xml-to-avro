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
 *
 * @author  Mike Pigott
 */
public class TestAvroToXmlAndBack {

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
    final File schemaFile = new File("src\\test\\resources\\test_schema.xsd");
    final File xmlFile = new File("src\\test\\resources\\test1_root.xml");

    final XmlDatumConfig config =
        new XmlDatumConfig(schemaFile, "http://avro.apache.org/AvroTest", root);

    runTest(config, xmlFile);
  }

  @Test
  public void testChildren() throws Exception {
    final QName root = new QName("http://avro.apache.org/AvroTest", "root");
    final File schemaFile = new File("src\\test\\resources\\test_schema.xsd");
    final File xmlFile = new File("src\\test\\resources\\test2_children.xml");

    final XmlDatumConfig config =
        new XmlDatumConfig(schemaFile, "http://avro.apache.org/AvroTest", root);

    runTest(config, xmlFile);
  }

  @Test
  public void testGrandchildren() throws Exception {
    final QName root = new QName("http://avro.apache.org/AvroTest", "root");
    final File schemaFile = new File("src\\test\\resources\\test_schema.xsd");
    final File xmlFile = new File("src\\test\\resources\\test3_grandchildren.xml");

    final XmlDatumConfig config =
        new XmlDatumConfig(schemaFile, "http://avro.apache.org/AvroTest", root);

    runTest(config, xmlFile);
  }

  @Test
  public void testComplex() throws Exception {
    final QName root = new QName("urn:avro:complex_schema", "root");
    final File complexSchemaFile = new File("src\\test\\resources\\complex_schema.xsd");
    final File testSchemaFile = new File("src\\test\\resources\\test_schema.xsd");
    final File xmlFile = new File("src\\test\\resources\\complex_test1.xml");

    final XmlDatumConfig config =
        new XmlDatumConfig(complexSchemaFile, "urn:avro:complex_schema", root);
    config.addSchemaFile(testSchemaFile);

    runTest(config, xmlFile);
  }

  private void runTest(XmlDatumConfig config, File xmlFile) throws Exception {
    final XmlDatumWriter writer = new XmlDatumWriter(config);
    final Schema xmlToAvroSchema = writer.getSchema();

    /*
    FileWriter tempSchemaWriter = new FileWriter("test.avsc");
    tempSchemaWriter.write( xmlToAvroSchema.toString(true) );
    tempSchemaWriter.close();
    */

    final Document xmlDoc = docBuilder.parse(xmlFile);

    final ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    final JsonEncoder encoder =
        avroEncoderFactory.jsonEncoder(xmlToAvroSchema, outStream, true);

    writer.write(xmlDoc, encoder);

    encoder.flush();

    /*
    BufferedReader tempReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(outStream.toByteArray())));
    PrintWriter tempWriter = new PrintWriter(new FileWriter("test.avro"));
    String line = null;
    while ((line = tempReader.readLine()) != null) {
      tempWriter.println(line);
    }
    tempWriter.close();
    */

    final ByteArrayInputStream inStream =
        new ByteArrayInputStream( outStream.toByteArray() );

    final JsonDecoder decoder =
        avroDecoderFactory.jsonDecoder(xmlToAvroSchema, inStream);

    final XmlDatumReader reader = new XmlDatumReader();
    reader.setSchema(xmlToAvroSchema);

    final Document outDoc = reader.read(null, decoder);

    DocumentComparer.assertEquivalent(xmlDoc, outDoc);
  }

  private DocumentBuilder docBuilder;

  private static DocumentBuilderFactory dbf;
  private static EncoderFactory avroEncoderFactory;
  private static DecoderFactory avroDecoderFactory;
}
