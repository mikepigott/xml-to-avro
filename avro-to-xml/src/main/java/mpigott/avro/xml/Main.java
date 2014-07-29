package mpigott.avro.xml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.stream.StreamSource;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.junit.Assert;

public class Main {

  public String getClasspathString() {
    StringBuffer classpath = new StringBuffer();
    ClassLoader applicationClassLoader = this.getClass().getClassLoader();
    if (applicationClassLoader == null) {
        applicationClassLoader = ClassLoader.getSystemClassLoader();
    }
    URL[] urls = ((URLClassLoader)applicationClassLoader).getURLs();
     for(int i=0; i < urls.length; i++) {
         classpath.append(urls[i].getFile()).append("\r\n");
     }    
    
     return classpath.toString();
 }

  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws Exception {
    Main main = new Main();
    System.out.println( main.getClasspathString() );

    /*
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    System.out.println();
    System.out.println("[Start jvisualvm, and press any key when ready.]");
    System.out.println();
    reader.readLine();
    */

    //GraphGenerationVisitor visitor = new GraphGenerationVisitor();
    //NullVisitor nullVisitor = new NullVisitor();
    AvroSchemaGenerator avroVisitor = new AvroSchemaGenerator();

    long startResolve = System.currentTimeMillis();
    long endResolve = 0;

    XmlSchemaCollection collection = null;
    try {
      //File file = new File("src\\test\\resources\\test_schema.xsd");
      //System.out.println("Reading from: " + file.getAbsolutePath());
      URL url = new URL("http://xbrl.fasb.org/us-gaap/2013/elts/us-gaap-2013-01-31.xsd");
      collection = new XmlSchemaCollection();
      collection.setSchemaResolver(new XmlSchemaMultiBaseUriResolver());
      collection.setBaseUri("http://xbrl.fasb.org/us-gaap/2013/elts/");
      collection.read(new StreamSource(url.openStream(), url.toString()));
      //FileReader fileReader = new FileReader(file);
      //collection.read(new StreamSource(fileReader, file.getAbsolutePath()));
      //fileReader.close();
    } finally {
      endResolve = System.currentTimeMillis();

      System.out.println("Schema resolution required " + (endResolve - startResolve) + " milliseconds.");
    }

    XmlSchemaWalker walker = new XmlSchemaWalker(collection, avroVisitor);

    /*
    long startContextWalk = System.currentTimeMillis();
    try {
      XmlSchemaElement context = getElementOf(collection, "context");
      walk(walker, avroVisitor, context, "context.avsc");
      //walker.walk(context);
    } finally {
      long endContextWalk = System.currentTimeMillis();
      System.out.println("Walking the context node took " + (endContextWalk - startContextWalk) + " milliseconds.");
    }

    long startCosWalk = System.currentTimeMillis();
    try {
      XmlSchemaElement costOfServices = getElementOf(collection, "CostOfServices");
      walk(walker, avroVisitor, costOfServices, "CostOfServices.avsc");
      //walker.walk(costOfServices);
    } finally {
      long endCosWalk = System.currentTimeMillis();
      System.out.println("Walking the CostOfServices node took " + (endCosWalk - startCosWalk) + " milliseconds.");
    }
    */

    long startXbrlWalk = System.currentTimeMillis();
    try {
      XmlSchemaElement xbrl = getElementOf(collection, "xbrl");
      walk(walker, avroVisitor, xbrl, "xbrl.avsc");
      XmlSchemaStateMachineGenerator smGen = new XmlSchemaStateMachineGenerator(avroVisitor.getSchema(), true);
      avroVisitor.clear();
      walker.removeVisitor(avroVisitor).addVisitor(smGen);
      walker.walk(xbrl);
      XmlSchemaStateMachineNode startNode = smGen.getStartNode();
      XmlToAvroPathCreator pathCreator = new XmlToAvroPathCreator(startNode);

      SAXParserFactory spf = SAXParserFactory.newInstance();
      spf.setNamespaceAware(true);
      SAXParser saxParser = spf.newSAXParser();
      final File xsdFile = new File("C:\\Users\\Mike Pigott\\Google Drive\\workspace\\edgar_xbrl\\src\\test\\resources\\fds-20130228.xml");
      saxParser.parse(xsdFile, pathCreator);

      XmlSchemaPathNode rootPath =
          pathCreator.getXmlSchemaDocumentPath();

    } finally {
      long endXbrlWalk = System.currentTimeMillis();
      System.out.println("Walking the xbrl node took " + (endXbrlWalk - startXbrlWalk) + " milliseconds.");
    }

    /*
    XmlSchemaElement root = getElementOf(collection, "root");
    //walk(walker, visitor, root, "test.dot");
    walk(walker, avroVisitor, root, "test.avsc");
    */
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

  private static void walk(XmlSchemaWalker walker, GraphGenerationVisitor visitor, XmlSchemaElement elem, String outFileName) throws IOException {
    walker.walk(elem);

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

    visitor.clear();
    walker.clear();
  }

  private static void walk(XmlSchemaWalker walker, AvroSchemaGenerator visitor, XmlSchemaElement elem, String outFileName) throws IOException {
    walker.walk(elem);
    Schema schema = visitor.getSchema();

    FileWriter writer = null;
    try {
      writer = new FileWriter(outFileName);
      writer.write( schema.toString(true) );
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }
    }

    walker.clear();
  }
}
