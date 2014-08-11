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

import java.util.List;

import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.XMLUnit;
import org.w3c.dom.Document;

/**
 * Compares two {@link Document}s for equivalence.
 *
 * @author  Mike Pigott
 */
final class DocumentComparer {

  static void assertEquivalent(Document expected, Document actual) {
    XMLUnit.setIgnoreWhitespace(true);
    XMLUnit.setIgnoreAttributeOrder(true);
    DetailedDiff diff = new DetailedDiff(XMLUnit.compareXML(expected, actual));

    List<?> allDifferences = diff.getAllDifferences();

    assertEquals(
        "Differences found: " + diff.toString(),
        0,
        allDifferences.size());
  }
}
