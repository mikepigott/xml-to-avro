package mpigott.avro.xml;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import javax.xml.transform.stream.StreamSource;

import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaAttributeOrGroupRef;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaComplexType;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.junit.Assert;
import org.junit.Test;

import junit.framework.TestCase;

public class GraphVisTest extends TestCase {

  @Test
  public void test() throws IOException {
    GraphGenerationVisitor visitor = new GraphGenerationVisitor();

    NullVisitor nullVisitor = new NullVisitor();

    long startResolve = System.currentTimeMillis();
    long endResolve = 0;

    XmlSchemaCollection collection = null;
    try {
      URL url = new URL("http://xbrl.fasb.org/us-gaap/2013/elts/us-gaap-2013-01-31.xsd");
      collection = new XmlSchemaCollection();
      collection.setSchemaResolver(new XmlSchemaMultiBaseUriResolver());
      collection.setBaseUri("http://xbrl.fasb.org/us-gaap/2013/elts/");
      InputStream urlStream = url.openStream();
      collection.read(new StreamSource(urlStream, url.toString()));
    } finally {
      endResolve = System.currentTimeMillis();

      System.out.println("Schema resolution required " + (endResolve - startResolve) + " milliseconds.");
    }

    XmlSchemaWalker walker = new XmlSchemaWalker(collection, nullVisitor);

    long startContextWalk = System.currentTimeMillis();
    try {
      XmlSchemaElement context = getElementOf(collection, "context");
      //walk(walker, visitor, context, "context.dot");
      walker.walk(context);
    } finally {
      long endContextWalk = System.currentTimeMillis();
      System.out.println("Walking the context node took " + (endContextWalk - startContextWalk) + " milliseconds.");
    }

    long startCosWalk = System.currentTimeMillis();
    try {
      XmlSchemaElement costOfServices = getElementOf(collection, "CostOfServices");
      //walk(walker, visitor, costOfServices, "CostOfServices.dot");
      walker.walk(costOfServices);
    } finally {
      long endCosWalk = System.currentTimeMillis();
      System.out.println("Walking the CostOfServices node took " + (endCosWalk - startCosWalk) + " milliseconds.");
    }

    long startXbrlWalk = System.currentTimeMillis();
    try {
      XmlSchemaElement xbrl = getElementOf(collection, "xbrl");
      //walk(walker, visitor, xbrl, "xbrl.dot");
      walker.walk(xbrl);
    } finally {
      long endXbrlWalk = System.currentTimeMillis();
      System.out.println("Walking the xbrl node took " + (endXbrlWalk - startXbrlWalk) + " milliseconds.");
    }

    //checkContextElem(context);
  }

  private XmlSchemaElement getElementOf(XmlSchemaCollection collection, String name) {
    XmlSchemaElement elem = null;
    for (XmlSchema schema : collection.getXmlSchemas()) {
      elem = schema.getElementByName(name);
      if (elem != null) {
        break;
      }
    }
    return elem;
  }

  private void walk(XmlSchemaWalker walker, GraphGenerationVisitor visitor, XmlSchemaElement elem, String outFileName) throws IOException {
    walker.walk(elem);
    Assert.assertTrue( visitor.clear() );

    FileWriter writer = null;
    try {
      writer = new FileWriter(outFileName);
      writer.write( visitor.toString() );
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }
    }
  }

  private void checkContextElem(XmlSchemaElement context) {
    XmlSchemaComplexType type = (XmlSchemaComplexType) context.getSchemaType();
    List<XmlSchemaAttributeOrGroupRef> attributes = type.getAttributes();
    Assert.assertEquals(1, attributes.size());
    XmlSchemaAttribute attr = (XmlSchemaAttribute) attributes.get(0);
    Assert.assertEquals("id", attr.getName());
    Assert.assertNotNull( attr.getSchemaType() );
  }
}
