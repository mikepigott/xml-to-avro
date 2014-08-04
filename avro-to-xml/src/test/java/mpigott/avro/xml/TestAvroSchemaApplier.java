package mpigott.avro.xml;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;

import mpigott.avro.xml.AvroSchemaGenerator;
import mpigott.avro.xml.XmlSchemaMultiBaseUriResolver;
import mpigott.avro.xml.XmlSchemaWalker;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

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

/**
 * Tests {@link AvroSchemaApplier}.
 *
 * @author  Mike Pigott
 */
public class TestAvroSchemaApplier {

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

    avroSchema = visitor.getSchema();

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
    pathCreator = new XmlSchemaPathFinder(root);
  }

  @Test
  public void test() throws Exception {
    // 1. Build the XML Document Path.
    final File xsdFile = new File("src\\test\\resources\\test3_grandchildren.xml");

    try {
      saxParser.parse(xsdFile, pathCreator);
    } catch (SAXException e) {
      e.printStackTrace();
      throw e;
    }

    XmlSchemaPathNode rootPath =
        pathCreator.getXmlSchemaDocumentPath();

    XmlSchemaDocumentNode<AvroRecordInfo> rootDoc = rootPath.getDocumentNode();

    assertNotNull(rootPath);
    assertNotNull(rootDoc);

    // 2. Confirm the Avro Schema conforms to the XML Schema
    AvroSchemaApplier applier = new AvroSchemaApplier(avroSchema, true);

    applier.apply(rootDoc);

    final int numElemsProcessed = checkDoc(rootDoc);
    assertEquals(7, numElemsProcessed);
  }

  private int checkDoc(XmlSchemaDocumentNode<AvroRecordInfo> doc) {
    int numElemsProcessed = 0;
    if (doc
          .getStateMachineNode()
          .getNodeType()
          .equals(XmlSchemaStateMachineNode.Type.ELEMENT)) {
      assertNotNull( doc.getUserDefinedContent() );

      final AvroRecordInfo recordInfo = doc.getUserDefinedContent();
      final Schema schema = recordInfo.getAvroSchema();
      assertTrue(
          schema.getType().equals(Schema.Type.RECORD)
          || schema.getType().equals(Schema.Type.MAP));

      assertEquals(
          doc.getStateMachineNode().getElement().getName(),
          schema.getName());

      ++numElemsProcessed;
    } else {
      assertNull( doc.getUserDefinedContent() );
    }

    for (int iter = 1; iter <= doc.getIteration(); ++iter) {
      final SortedMap<Integer, XmlSchemaDocumentNode<AvroRecordInfo>>
        children = doc.getChildren(iter);

      if (children != null) {
        for (Map.Entry<Integer, XmlSchemaDocumentNode<AvroRecordInfo>> child :
              children.entrySet()) {
          numElemsProcessed += checkDoc( child.getValue() );
        }
      }
    }
    return numElemsProcessed;
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
  private XmlSchemaPathFinder pathCreator;

  private static XmlSchemaStateMachineNode root;
  private static SAXParserFactory spf;
  private static Schema avroSchema;
}
