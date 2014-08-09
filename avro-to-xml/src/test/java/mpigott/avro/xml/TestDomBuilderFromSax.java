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

import org.junit.Test;
import org.w3c.dom.Document;

/**
 * Tests {@link DomBuilderFromSax}.
 *
 * @author  Mike Pigott
 */
public class TestDomBuilderFromSax {

  @Test
  public void test() throws Exception {
    final File xmlFile = new File("src\\test\\resources\\test3_grandchildren.xml");

    /* Parse the document using a real DOM parser
    final DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    dbFactory.setNamespaceAware(true);
    final DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    final Document doc = dBuilder.parse(xmlFile);
    */

    // Parse the document using a SAX parser
    DomBuilderFromSax builder = new DomBuilderFromSax(null);

    SAXParserFactory spf = SAXParserFactory.newInstance();
    spf.setNamespaceAware(true);
    SAXParser saxParser = spf.newSAXParser();
    saxParser.parse(xmlFile, builder);

    final Document outDoc = builder.getDocument();

    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Transformer transformer = transformerFactory.newTransformer();
    DOMSource source = new DOMSource(outDoc);
 
    // Output to console for testing
    StreamResult result = new StreamResult(System.out);

    transformer.transform(source, result);
  }

}
