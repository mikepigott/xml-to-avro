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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;

/**
 * Tests {@link DomBuilderFromSax}.
 *
 * @author  Mike Pigott
 */
public class TestDomBuilderFromSax {

  @BeforeClass
  public static void setUpFactories() {
    dbf = DocumentBuilderFactory.newInstance();
    dbf.setNamespaceAware(true);

    spf = SAXParserFactory.newInstance();
    spf.setNamespaceAware(true);
  }

  @Before
  public void setUpTest() throws Exception {
    saxParser = spf.newSAXParser();
    domParser = dbf.newDocumentBuilder();
  }

  @Test
  public void testSchema() throws Exception {
    runTest(new File("src\\test\\resources\\test_schema.xsd"));
  }

  @Test
  public void testRoot() throws Exception {
    runTest(new File("src\\test\\resources\\test1_root.xml"));
  }

  @Test
  public void testChildren() throws Exception {
    runTest(new File("src\\test\\resources\\test2_children.xml"));
  }

  @Test
  public void testGrandchildren() throws Exception {
    runTest(new File("src\\test\\resources\\test3_grandchildren.xml"));
  }

  private void runTest(File xmlFile) throws Exception {
    // Parse the document using a real DOM parser
    final Document expectedDoc = domParser.parse(xmlFile);

    // Parse the document using a SAX parser
    DomBuilderFromSax builder = new DomBuilderFromSax(null);
    saxParser.parse(xmlFile, builder);

    final Document actualDoc = builder.getDocument();

    DocumentComparer.assertEquivalent(expectedDoc, actualDoc);
  }

  private SAXParser saxParser;
  private DocumentBuilder domParser;

  private static SAXParserFactory spf;
  private static DocumentBuilderFactory dbf;
}
