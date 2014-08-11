package mpigott.avro.xml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.junit.Assert;
import org.w3c.dom.Document;

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

  public static void main(String[] args) throws Exception {
    final XmlDatumConfig config = new XmlDatumConfig(new URL("http://xbrl.fasb.org/us-gaap/2012/elts/us-gaap-2012-01-31.xsd"), new QName("http://www.xbrl.org/2003/instance", "xbrl"));
    config.addSchemaUrl(new URL("http://www.sec.gov/Archives/edgar/data/1013237/000143774913004187/fds-20130228.xsd"));
    config.addSchemaUrl(new URL("http://xbrl.sec.gov/dei/2012/dei-2012-01-31.xsd"));

    final XmlDatumWriter writer = new XmlDatumWriter(config);

    Schema avroSchema = writer.getSchema();

    FileWriter schemaWriter = new FileWriter("xbrl.avsc");
    schemaWriter.write( avroSchema.toString(true) );
    schemaWriter.close();

    final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setNamespaceAware(true);
    final DocumentBuilder db = dbf.newDocumentBuilder();
    final File xbrlFile = new File("C:\\Users\\Mike Pigott\\Google Drive\\workspace\\edgar_xbrl\\src\\test\\resources\\fds-20130228.xml");
    final Document xbrlDoc = db.parse(xbrlFile);

    final EncoderFactory ef = EncoderFactory.get();

    final FileOutputStream outStream = new FileOutputStream("xbrl.avro");

    final JsonEncoder encoder = ef.jsonEncoder(avroSchema, outStream, true);

    writer.write(xbrlDoc, encoder);

    encoder.flush();

    outStream.close();
    // */
    Schema.Parser parser = new Schema.Parser();

    /*Schema */avroSchema = parser.parse(new File("xbrl.avsc"));

    DecoderFactory df = DecoderFactory.get();

    FileInputStream inStream = new FileInputStream("xbrl.avro");

    JsonDecoder jd = df.jsonDecoder(avroSchema, inStream);

    XmlDatumReader reader = new XmlDatumReader();
    reader.setSchema(avroSchema);

    final Document doc = reader.read(null, jd);

    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Transformer transformer = transformerFactory.newTransformer();
    DOMSource source = new DOMSource(doc);
 
    // Output to console for testing
    StreamResult result = new StreamResult(new FileOutputStream("new_xbrl.xml"));

    transformer.transform(source, result);

    inStream.close();
  }

  /**
   * @param args
   * @throws IOException 
   */
  public static void oldMain(String[] args) throws Exception {
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
    ArrayList<URL> schemaUrls = new ArrayList<URL>(3);
    final String baseUri = "http://xbrl.fasb.org/us-gaap/2013/elts/";

    long startResolve = System.currentTimeMillis();
    long endResolve = 0;

    XmlSchemaCollection collection = null;
    try {
      //File file = new File("src\\test\\resources\\test_schema.xsd");
      //System.out.println("Reading from: " + file.getAbsolutePath());
      // http://xbrl.fasb.org/us-gaap/2013/elts/us-gaap-2013-01-31.xsd
      // http://xbrl.fasb.org/us-gaap/2012/elts/us-gaap-2012-01-31.xsd
      // http://www.sec.gov/Archives/edgar/data/1013237/000143774913004187/fds-20130228.xsd
      URL url = new URL("http://xbrl.fasb.org/us-gaap/2012/elts/us-gaap-2012-01-31.xsd");
      URL fdsUrl = new URL("http://www.sec.gov/Archives/edgar/data/1013237/000143774913004187/fds-20130228.xsd");
      URL deiUrl = new URL("http://xbrl.sec.gov/dei/2012/dei-2012-01-31.xsd");

      schemaUrls.add(url);
      schemaUrls.add(fdsUrl);
      schemaUrls.add(deiUrl);

      collection = new XmlSchemaCollection();
      collection.setSchemaResolver(new XmlSchemaMultiBaseUriResolver());
      collection.setBaseUri(baseUri);
      collection.read(new StreamSource(url.openStream(), url.toString()));
      collection.read(new StreamSource(fdsUrl.openStream(), fdsUrl.toString()));
      collection.read(new StreamSource(deiUrl.openStream(), deiUrl.toString()));
      //FileReader fileReader = new FileReader(file);
      //collection.read(new StreamSource(fileReader, file.getAbsolutePath()));
      //fileReader.close();
      System.out.println("Parsed " + collection.getXmlSchemas().length + " XML schemas.");
    } finally {
      endResolve = System.currentTimeMillis();

      System.out.println("Schema resolution required " + (endResolve - startResolve) + " milliseconds.");
    }

    AvroSchemaGenerator avroVisitor =
        new AvroSchemaGenerator(baseUri, schemaUrls, null);

    XmlSchemaWalker walker = new XmlSchemaWalker(collection, avroVisitor);
    walker.setUserRecognizedTypes( Utils.getAvroRecognizedTypes() );

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
    XmlSchemaPathNode rootPath = null;
    long startXbrlWalk = System.currentTimeMillis();
    try {
      XmlSchemaElement xbrl = getElementOf(collection, "xbrl");
      //walk(walker, avroVisitor, xbrl, "xbrl.avsc");
      XmlSchemaStateMachineGenerator smGen = new XmlSchemaStateMachineGenerator();
      ///avroVisitor.clear();
      walker.removeVisitor(avroVisitor).addVisitor(smGen);
      walker.walk(xbrl);
      XmlSchemaStateMachineNode startNode = smGen.getStartNode();

      visualizeStateMachine(startNode, "xbrl_sm.dot");

      XmlSchemaPathFinder pathCreator = new XmlSchemaPathFinder(startNode);
      SAXParserFactory spf = SAXParserFactory.newInstance();
      spf.setNamespaceAware(true);
      SAXParser saxParser = spf.newSAXParser();
      final File xsdFile = new File("C:\\Users\\Mike Pigott\\Google Drive\\workspace\\edgar_xbrl\\src\\test\\resources\\fds-20130228.xml");
      saxParser.parse(xsdFile, pathCreator);
      rootPath = pathCreator.getXmlSchemaDocumentPath();
    } finally {
      long endXbrlWalk = System.currentTimeMillis();
      System.out.println("Walking the xbrl node took " + (endXbrlWalk - startXbrlWalk) + " milliseconds.");
    }

    XmlSchemaPathNode iter = rootPath;
    XmlSchemaPathNode.Direction prevDirection = XmlSchemaPathNode.Direction.CHILD;
    int nodeCount = 0;
    do {
      if (!prevDirection.equals(XmlSchemaPathNode.Direction.CONTENT) || !iter.getDirection().equals(XmlSchemaPathNode.Direction.CONTENT)) {
        System.out.println(iter.getDirection() + " " + iter.getStateMachineNode() + " " + iter.getIteration());
      }
      prevDirection = iter.getDirection();
      iter = iter.getNext();
      ++nodeCount;
    } while (iter != null);

    System.out.println("Path contained " + nodeCount + " nodes.");

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

  private static void visualizeStateMachine(XmlSchemaStateMachineNode startNode, String fileName) throws Exception {
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

    nextNode(startNode, 0, nodes, edges, templates, new HashMap<QName, Integer>());

    StringTemplate fileSt = templates.getInstanceOf("file");
    fileSt.setAttribute("gname", "state_machine");
    fileSt.setAttribute("nodes", nodes);
    fileSt.setAttribute("edges", edges);

    FileWriter writer = null;
    try {
      writer = new FileWriter(fileName);
      writer.write( fileSt.toString() ); 
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

  private static int nextNode(XmlSchemaStateMachineNode currNode, int nodeNum, ArrayList<StringTemplate> nodes, ArrayList<StringTemplate> edges, StringTemplateGroup templates, Map<QName, Integer> nodeNums) {
    int nextNum = nodeNum + 1;

    nodes.add( getNodeSt(templates, "node" + nodeNum, currNode.toString()) );

    if ( currNode.getNodeType().equals(XmlSchemaStateMachineNode.Type.ELEMENT) ) {
      nodeNums.put(currNode.getElement().getQName(), nodeNum);
    }

    if (currNode.getPossibleNextStates() != null) {
      for (XmlSchemaStateMachineNode nextNode : currNode.getPossibleNextStates()) {
        int nextNodeNum = nextNum;

        if (nextNode.getNodeType().equals(XmlSchemaStateMachineNode.Type.ELEMENT)
            && nodeNums.containsKey( nextNode.getElement().getQName() )) {
          nextNodeNum = nodeNums.get( nextNode.getElement().getQName() );
        } else {
          nextNum = nextNode(nextNode, nextNum, nodes, edges, templates, nodeNums);
        }
        edges.add( getEdgeSt(templates, "node" + nodeNum, "node" + nextNodeNum) );
      }
    }

    return nextNum;
  }

  private static StringTemplate getEdgeSt(StringTemplateGroup templates, String from, String to) {
    StringTemplate edgeSt = templates.getInstanceOf("edge");
    edgeSt.setAttribute("from", from);
    edgeSt.setAttribute("to", to);
    return edgeSt;
  }

  private static StringTemplate getNodeSt(StringTemplateGroup templates, String name, String text) {
    StringTemplate tmpl = templates.getInstanceOf("node");
    tmpl.setAttribute("name", name);
    tmpl.setAttribute("text", text.replace('\"', '\''));
    return tmpl;
  }
}
