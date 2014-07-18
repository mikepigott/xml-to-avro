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

import javax.xml.transform.stream.StreamSource;

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
  public static void main(String[] args) throws IOException {
    Main main = new Main();
    System.out.println( main.getClasspathString() );
    System.out.println();
    System.out.println("[Start jvisualvm, and press any key when ready.]");
    System.out.println();

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    reader.readLine();

    GraphGenerationVisitor visitor = new GraphGenerationVisitor();
    NullVisitor nullVisitor = new NullVisitor();

    long startResolve = System.currentTimeMillis();
    long endResolve = 0;

    XmlSchemaCollection collection = null;
    try {
      File file = new File("src\\test\\resources\\test_schema.xsd");
      System.out.println("Reading from: " + file.getAbsolutePath());
      URL url = new URL("http://xbrl.fasb.org/us-gaap/2013/elts/us-gaap-2013-01-31.xsd");
      collection = new XmlSchemaCollection();
      collection.setSchemaResolver(new XmlSchemaMultiBaseUriResolver());
      //collection.setBaseUri("http://xbrl.fasb.org/us-gaap/2013/elts/");
      //InputStream urlStream = url.openStream();
      FileReader fileReader = new FileReader(file);
      collection.read(new StreamSource(fileReader, file.getAbsolutePath()));
      fileReader.close();
    } finally {
      endResolve = System.currentTimeMillis();

      System.out.println("Schema resolution required " + (endResolve - startResolve) + " milliseconds.");
    }

    XmlSchemaWalker walker = new XmlSchemaWalker(collection, visitor);

    /*
    long startContextWalk = System.currentTimeMillis();
    try {
      XmlSchemaElement context = getElementOf(collection, "context");
      walk(walker, visitor, context, "context.dot");
      //walker.walk(context);
    } finally {
      long endContextWalk = System.currentTimeMillis();
      System.out.println("Walking the context node took " + (endContextWalk - startContextWalk) + " milliseconds.");
    }

    long startCosWalk = System.currentTimeMillis();
    try {
      XmlSchemaElement costOfServices = getElementOf(collection, "CostOfServices");
      walk(walker, visitor, costOfServices, "CostOfServices.dot");
      //walker.walk(costOfServices);
    } finally {
      long endCosWalk = System.currentTimeMillis();
      System.out.println("Walking the CostOfServices node took " + (endCosWalk - startCosWalk) + " milliseconds.");
    }

    long startXbrlWalk = System.currentTimeMillis();
    try {
      XmlSchemaElement xbrl = getElementOf(collection, "xbrl");
      walk(walker, visitor, xbrl, "xbrl.dot");
      //walker.walk(xbrl);
    } finally {
      long endXbrlWalk = System.currentTimeMillis();
      System.out.println("Walking the xbrl node took " + (endXbrlWalk - startXbrlWalk) + " milliseconds.");
    }
    */

    XmlSchemaElement root = getElementOf(collection, "root");
    walk(walker, visitor, root, "test.dot");
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
  }
}
