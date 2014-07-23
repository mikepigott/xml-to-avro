package mpigott.avro.xml;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import javax.xml.transform.stream.StreamSource;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.junit.Test;

public class TestSchemaStateMachineGenerator {

  @Test
  public void test() throws Exception {
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

    // 2. Confirm the Avro Schema conforms to the XML Schema
    SchemaStateMachineGenerator generator =
        new SchemaStateMachineGenerator(schema, true);

    walker.removeVisitor(visitor).addVisitor(generator);

    walker.walk(elem);

    assertNotNull( generator.getStartNode() );
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
}
