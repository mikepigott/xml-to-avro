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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
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

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.xml.XmlDatumConfig;
import org.apache.avro.xml.XmlDatumReader;
import org.apache.avro.xml.XmlDatumWriter;
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

  public static void numericMain(String[] args) throws Exception {
    BigInteger value = new BigInteger("123456789123456789123456789");
    BigDecimal decimal = new BigDecimal(value, Integer.MIN_VALUE);
    System.out.println(decimal);
    System.out.println( MathContext.DECIMAL128.getPrecision() );
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
}
