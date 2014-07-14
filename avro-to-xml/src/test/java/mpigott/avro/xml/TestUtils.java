package mpigott.avro.xml;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import junit.framework.Assert;
import junit.framework.TestCase;

public class TestUtils extends TestCase {

    public void testGetAvroNamespaceForString() throws URISyntaxException {
	Assert.assertEquals(EXPECTED_RESULT, Utils.getAvroNamespaceFor(NAMESPACE_URI));
    }

    public void testGetAvroNamespaceForURL() throws MalformedURLException, URISyntaxException {
	Assert.assertEquals(EXPECTED_RESULT, Utils.getAvroNamespaceFor(new URL(NAMESPACE_URI)));
    }

    public void testGetAvroNamespaceForURI() throws URISyntaxException {
	Assert.assertEquals(EXPECTED_RESULT, Utils.getAvroNamespaceFor(new URI(NAMESPACE_URI)));
    }

    private static String NAMESPACE_URI = "http://www.sec.gov/Archives/edgar/data/1013237/000143774913004187/fds-20130228.xsd";
    private static String EXPECTED_RESULT = "gov.sec.www.Archives.edgar.data.1013237.000143774913004187.fds-20130228.xsd";
}
