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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 * Tests the {@link XmlToAvroPathCreator} with XML documents following
 * the <code>src/test/resources/test_schema.xsd</code> XML Schema.
 *
 * @author  Mike Pigott
 */
public class TestXmlToAvroPathCreator {

  @BeforeClass
  public static void createStateMachine() throws FileNotFoundException {
    // 1. Construct the Avro Schema
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
    walker.walk(elem);

    Schema schema = visitor.getSchema();

    visitor.clear();
    walker.clear();

    // 2. Construct the state machine.
    SchemaStateMachineGenerator generator =
        new SchemaStateMachineGenerator(schema, true);

    walker.removeVisitor(visitor).addVisitor(generator);

    walker.walk(elem);

    root = generator.getStartNode();

    spf = SAXParserFactory.newInstance();
    spf.setNamespaceAware(true);
  }

  @Before
  public void createSaxParser() throws Exception {
    saxParser = spf.newSAXParser();
    pathCreator = new XmlToAvroPathCreator(root);
  }

  @Test // @Ignore
  public void testRoot() throws Exception {
    final File xsdFile = new File("src\\test\\resources\\test1_root.xml");
    saxParser.parse(xsdFile, pathCreator);

    XmlSchemaDocumentPathNode rootPath =
        pathCreator.getXmlSchemaDocumentPath();

    XmlSchemaDocumentNode rootDoc =
        pathCreator.getXmlSchemaDocumentRoot();

    assertNotNull(rootPath);
    assertNotNull(rootDoc);

    assertTrue(
        (rootDoc.getChildren() == null)
        || (rootDoc.getChildren().isEmpty()));

    assertEquals(1, rootDoc.getCurrIteration());
    assertEquals(-1, rootDoc.getCurrPositionInSequence());
    assertNull( rootDoc.getParent() );
    assertFalse(rootDoc.getReceivedContent());
    assertNotNull(rootDoc.getStateMachineNode());

    assertEquals(
        SchemaStateMachineNode.Type.ELEMENT,
        rootDoc.getStateMachineNode().getNodeType());

    assertTrue(rootDoc.getStateMachineNode() == root);

    assertEquals(
        XmlSchemaDocumentPathNode.Direction.CHILD,
        rootPath.getDirection());

    assertTrue(rootPath.getDocumentNode() == rootDoc);
    assertEquals(-1, rootPath.getIndexOfNextNodeState());

    assertEquals(1, rootPath.getIteration());

    assertNull(rootPath.getPrevious());
    assertEquals(rootPath.getPriorSequencePosition(), -1);
    assertTrue(rootPath.getStateMachineNode() == root);
    assertNull( rootPath.getNext() );
  }

  @Test
  public void testChildren() throws Exception {
    final File xsdFile = new File("src\\test\\resources\\test2_children.xml");

    try {
      saxParser.parse(xsdFile, pathCreator);
    } catch (SAXException e) {
      e.printStackTrace();
      throw e;
    }

    XmlSchemaDocumentPathNode rootPath =
        pathCreator.getXmlSchemaDocumentPath();

    XmlSchemaDocumentNode rootDoc =
        pathCreator.getXmlSchemaDocumentRoot();

    assertNotNull(rootPath);
    assertNotNull(rootDoc);

    assertTrue(
        (rootDoc.getChildren() != null)
        && !rootDoc.getChildren().isEmpty());

    for (int i = 0; i < rootDoc.getChildren().size(); ++i) {
      System.out.println( rootDoc.getChildren().get(i).getStateMachineNode().getNodeType() );
    }

    assertEquals(1, rootDoc.getChildren().size());
    assertEquals(1, rootDoc.getCurrIteration());
    assertEquals(-1, rootDoc.getCurrPositionInSequence());
    assertNull( rootDoc.getParent() );
    assertFalse(rootDoc.getReceivedContent());
    assertNotNull(rootDoc.getStateMachineNode());

    assertEquals(
        SchemaStateMachineNode.Type.ELEMENT,
        rootDoc.getStateMachineNode().getNodeType());

    assertTrue(rootDoc.getStateMachineNode() == root);

    assertEquals(
        XmlSchemaDocumentPathNode.Direction.CHILD,
        rootPath.getDirection());

    assertTrue(rootPath.getDocumentNode() == rootDoc);
    assertEquals(-1, rootPath.getIndexOfNextNodeState());

    assertEquals(1, rootPath.getIteration());

    assertNull(rootPath.getPrevious());
    assertEquals(rootPath.getPriorSequencePosition(), -1);
    assertTrue(rootPath.getStateMachineNode() == root);
    assertNotNull( rootPath.getNext() );

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

  private SAXParser saxParser;
  private XmlToAvroPathCreator pathCreator;

  private static SchemaStateMachineNode root;
  private static SAXParserFactory spf;
}
