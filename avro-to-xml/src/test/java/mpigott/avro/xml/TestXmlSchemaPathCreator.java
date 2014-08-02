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
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.namespace.QName;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
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
 * Tests the {@link XmlSchemaPathCreator} with XML documents following
 * the <code>src/test/resources/test_schema.xsd</code> XML Schema.
 *
 * @author  Mike Pigott
 */
public class TestXmlSchemaPathCreator {

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
    walker.setUserRecognizedTypes( Utils.getAvroRecognizedTypes() );
    walker.walk(elem);

    Schema schema = visitor.getSchema();

    visitor.clear();
    walker.clear();

    // 2. Construct the state machine.
    XmlSchemaStateMachineGenerator generator =
        new XmlSchemaStateMachineGenerator();

    walker.removeVisitor(visitor).addVisitor(generator);

    walker.walk(elem);

    root = generator.getStartNode();

    spf = SAXParserFactory.newInstance();
    spf.setNamespaceAware(true);
  }

  @Before
  public void createSaxParser() throws Exception {
    saxParser = spf.newSAXParser();
    pathCreator = new XmlSchemaPathCreator(root);
  }

  @Test
  public void testRoot() throws Exception {
    final File xsdFile = new File("src\\test\\resources\\test1_root.xml");
    saxParser.parse(xsdFile, pathCreator);

    XmlSchemaPathNode rootPath =
        pathCreator.getXmlSchemaDocumentPath();

    XmlSchemaDocumentNode rootDoc =
        rootPath.getDocumentNode();

    assertNotNull(rootPath);
    assertNotNull(rootDoc);

    assertTrue(
        (rootDoc.getChildren() == null)
        || (rootDoc.getChildren().isEmpty()));

    assertEquals(1, rootDoc.getIteration());
    assertEquals(-1, rootDoc.getSequencePosition());
    assertNull( rootDoc.getParent() );
    assertFalse(rootDoc.getReceivedContent());
    assertNotNull(rootDoc.getStateMachineNode());

    assertEquals(
        XmlSchemaStateMachineNode.Type.ELEMENT,
        rootDoc.getStateMachineNode().getNodeType());

    assertTrue(rootDoc.getStateMachineNode() == root);

    assertEquals(
        XmlSchemaPathNode.Direction.CHILD,
        rootPath.getDirection());

    assertTrue(rootPath.getDocumentNode() == rootDoc);
    assertEquals(-1, rootPath.getIndexOfNextNodeState());

    assertEquals(1, rootPath.getIteration());

    assertNull(rootPath.getPrevious());
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

    XmlSchemaPathNode rootPath =
        pathCreator.getXmlSchemaDocumentPath();

    XmlSchemaDocumentNode rootDoc = rootPath.getDocumentNode();

    assertNotNull(rootPath);
    assertNotNull(rootDoc);

    assertTrue(
        (rootDoc.getChildren() != null)
        && !rootDoc.getChildren().isEmpty());

    assertEquals(1, rootDoc.getChildren().size());
    assertEquals(1, rootDoc.getIteration());
    assertEquals(-1, rootDoc.getSequencePosition());
    assertNull( rootDoc.getParent() );
    assertFalse(rootDoc.getReceivedContent());
    assertNotNull(rootDoc.getStateMachineNode());

    assertEquals(
        XmlSchemaStateMachineNode.Type.ELEMENT,
        rootDoc.getStateMachineNode().getNodeType());

    assertTrue(rootDoc.getStateMachineNode() == root);

    assertEquals(
        XmlSchemaPathNode.Direction.CHILD,
        rootPath.getDirection());

    assertTrue(rootPath.getDocumentNode() == rootDoc);
    assertEquals(0, rootPath.getIndexOfNextNodeState());

    assertEquals(1, rootPath.getIteration());

    assertNull(rootPath.getPrevious());
    assertTrue(rootPath.getStateMachineNode() == root);
    assertNotNull( rootPath.getNext() );

    /* To replace with something more sophisticated: draw a graph of the walked path.
    StringTemplateGroup templates = null;
    FileReader fr = null;
    try {
      fr = new FileReader("C:\\Users\\Mike Pigott\\Google Drive\\workspace\\edgar_xbrl\\src\\main\\resources\\DOT.stg");
      templates = new StringTemplateGroup(fr);
    } finally {
      try {
        if (fr != null) {
          fr.close();
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    ArrayList<StringTemplate> nodes = new ArrayList<StringTemplate>();
    ArrayList<StringTemplate> edges = new ArrayList<StringTemplate>();

    nextNode(rootPath, 0, nodes, edges, templates);

    StringTemplate fileSt = templates.getInstanceOf("file");
    fileSt.setAttribute("gname", "walked_path");
    fileSt.setAttribute("nodes", nodes);
    fileSt.setAttribute("edges", edges);

    System.out.println( fileSt.toString() );
    */
  }

  @Test
  public void testGrandchildren() throws Exception {
    final File xsdFile = new File("src\\test\\resources\\test3_grandchildren.xml");

    try {
      saxParser.parse(xsdFile, pathCreator);
    } catch (SAXException e) {
      e.printStackTrace();
      throw e;
    }

    XmlSchemaPathNode rootPath =
        pathCreator.getXmlSchemaDocumentPath();

    XmlSchemaDocumentNode rootDoc = rootPath.getDocumentNode();

    assertNotNull(rootPath);
    assertNotNull(rootDoc);

    // To replace with something more sophisticated: draw a graph of the walked path.
    StringTemplateGroup templates = null;
    FileReader fr = null;
    try {
      fr = new FileReader("C:\\Users\\Mike Pigott\\Google Drive\\workspace\\edgar_xbrl\\src\\main\\resources\\DOT.stg");
      templates = new StringTemplateGroup(fr);
    } finally {
      try {
        if (fr != null) {
          fr.close();
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }
    }

    ArrayList<StringTemplate> nodes = new ArrayList<StringTemplate>();
    ArrayList<StringTemplate> edges = new ArrayList<StringTemplate>();

    nextNode(rootPath, 0, nodes, edges, templates);

    StringTemplate fileSt = templates.getInstanceOf("file");
    fileSt.setAttribute("gname", "walked_path");
    fileSt.setAttribute("nodes", nodes);
    fileSt.setAttribute("edges", edges);

    System.out.println( fileSt.toString() );

  }

  private int nextNode(XmlSchemaPathNode currNode, int nodeNum, ArrayList<StringTemplate> nodes, ArrayList<StringTemplate> edges, StringTemplateGroup templates) {
    int nextNum = nodeNum + 1;

    StringBuilder nodeName = new StringBuilder( currNode.getDirection().toString() );
    nodeName.append(' ').append( currNode.getStateMachineNode() );
    nodeName.append(" [Iteration: ").append( currNode.getIteration() ).append(']');
    nodeName.append(" [Next Node").append( currNode.getIndexOfNextNodeState() ).append(']');

    nodes.add( getNodeSt(templates, "node" + nodeNum, nodeName.toString()) );

    if (currNode.getNext() != null) {
      edges.add( getEdgeSt(templates, "node" + nodeNum, "node" + nextNum) );
      return nextNode(currNode.getNext(), nextNum, nodes, edges, templates);
    } else {
      return nextNum;
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

  private StringTemplate getEdgeSt(StringTemplateGroup templates, String from, String to) {
    StringTemplate edgeSt = templates.getInstanceOf("edge");
    edgeSt.setAttribute("from", from);
    edgeSt.setAttribute("to", to);
    return edgeSt;
  }

  private StringTemplate getNodeSt(StringTemplateGroup templates, String name, String text) {
    StringTemplate tmpl = templates.getInstanceOf("node");
    tmpl.setAttribute("name", name);
    tmpl.setAttribute("text", text.replace('\"', '\''));
    return tmpl;
  }

  private SAXParser saxParser;
  private XmlSchemaPathCreator pathCreator;

  private static XmlSchemaStateMachineNode root;
  private static SAXParserFactory spf;
}
