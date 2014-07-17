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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.Test;

import org.junit.Assert;

public class TestUtils {

  @Test
  public void testGetAvroNamespaceForString() throws URISyntaxException {
    Assert.assertEquals(EXPECTED_RESULT, Utils.getAvroNamespaceFor(NAMESPACE_URI));
  }

  @Test
  public void testGetAvroNamespaceForURL() throws MalformedURLException, URISyntaxException {
    Assert.assertEquals(EXPECTED_RESULT, Utils.getAvroNamespaceFor(new URL(NAMESPACE_URI)));
  }

  @Test
  public void testGetAvroNamespaceForURI() throws URISyntaxException {
    Assert.assertEquals(EXPECTED_RESULT, Utils.getAvroNamespaceFor(new URI(NAMESPACE_URI)));
  }

  private static String NAMESPACE_URI = "http://www.sec.gov/Archives/edgar/data/1013237/000143774913004187/fds-20130228.xsd";
  private static String EXPECTED_RESULT = "gov.sec.www.Archives.edgar.data.1013237.000143774913004187.fds-20130228.xsd";
}
