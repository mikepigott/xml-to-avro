/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.xml;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.avro.Schema;
import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.XMLUnit;
import org.w3c.dom.Document;

/**
 * Utilities to:
 *
 * <ul>
 *   <li>Compare two {@link Document}s for equivalence.</li>
 *   <li>Compare two Avro {@link Schema}s for equivalence</li>
 *   <li>Build a file path in a cross-platform way.</li>
 * </ul>
 */
final class UtilsForTests {

  static void assertEquivalent(Document expected, Document actual) {
    XMLUnit.setIgnoreWhitespace(true);
    XMLUnit.setIgnoreAttributeOrder(true);

    DetailedDiff diff = new DetailedDiff(XMLUnit.compareXML(expected, actual));

    assertTrue(
        "Differences found: " + diff.toString(),
        diff.similar());
  }

  static void assertEquivalent(Schema expected, Schema actual) {
    assertEquivalent(expected, actual, new HashSet<String>());
  }

  private static void assertEquivalent(
      Schema expected,
      Schema actual,
      HashSet<String> recordsSeen) {

    assertEquals(expected.getFullName() + " vs. " + actual.getFullName(),
                 expected.getType(),
                 actual.getType());

    switch ( expected.getType() ) {
    case ARRAY:
      assertEquivalent(
          expected.getElementType(),
          actual.getElementType(),
          recordsSeen);
      break;
    case MAP:
      assertEquivalent(
          expected.getValueType(),
          actual.getValueType(),
          recordsSeen);
      break;
    case UNION:
      {
        List<Schema> expSchemas = expected.getTypes();
        HashMap<String, Schema> expFullNames = new HashMap<String, Schema>();
        for (Schema expSchema : expSchemas) {
          expFullNames.put(expSchema.getFullName(), expSchema);
        }
        for (Schema actualSchema : actual.getTypes()) {
          assertTrue(
              "Cannot find field "
              + actualSchema.getFullName()
              + " in "
              + expected.getFullName(),
              expFullNames.containsKey( actualSchema.getFullName() ) );

          Schema expSchema = expFullNames.remove( actualSchema.getFullName() );
          assertEquivalent(expSchema, actualSchema, recordsSeen);
        }
        assertTrue( expFullNames.isEmpty() );
        break;
      }
    case FIXED:
      {
        assertEquals(expected.getFullName(), actual.getFullName());
        assertEquals(expected.getFixedSize(), actual.getFixedSize());
        break;
      }
    case ENUM:
      {
        assertEquals(expected.getFullName(), actual.getFullName());
        final int numActualSymbols = actual.getEnumSymbols().size();
        final List<String> expectedSymbols = expected.getEnumSymbols();
        assertEquals(expectedSymbols.size(), numActualSymbols);
        for (String expSym : expectedSymbols) {
          try {
            actual.getEnumOrdinal(expSym);
          } catch (Exception e) {
            fail("Expected Symbol \""
                 + expSym
                 + "\" in enum "
                 + expected.getFullName()
                 + " has no equivalent in actual.");
          }
        }
        break;
      }
    case RECORD:
      {
        assertEquals(expected.getFullName(), actual.getFullName());

        // Prevents infinite recursive descent.
        if ( !recordsSeen.contains( expected.getFullName() ) ) {
          recordsSeen.add( expected.getFullName() );

          List<Schema.Field> expFields = expected.getFields();

          if (expFields.size() != actual.getFields().size()) {
            HashSet<String> expectedFields = new HashSet<String>();
            for (Schema.Field expField : expFields) {
              expectedFields.add(expField.name());
            }

            HashSet<String> actualFields = new HashSet<String>();
            for (Schema.Field actualField : actual.getFields()) {
              actualFields.add(actualField.name());
            }

            List<String> foundExpectedFields = new ArrayList<String>();
            for (String expField : expectedFields) {
              if (actualFields.contains(expField)) {
                foundExpectedFields.add(expField);
                actualFields.remove(expField);
              }
            }
            for (String foundExpectedField : foundExpectedFields) {
              expectedFields.remove(foundExpectedField);
            }

            System.err.println("Missing Expected Fields:");
            for (String expField : expectedFields) {
              System.err.println('\t' + expField);
            }
            System.err.println("Unexpected Actual Fields:");
            for (String actField : actualFields) {
              System.err.println('\t' + actField);
            }
          }

          assertEquals(
              expected.getFullName(),
              expFields.size(),
              actual.getFields().size());

          for (Schema.Field expField : expFields) {
            Schema.Field actualField = actual.getField( expField.name() );
            assertNotNull(expected.getFullName() + " field " + expField.name(),
                          actualField);

            assertEquals(expected.getFullName() + " field " + expField.name(),
                         expField.doc(),
                         actualField.doc());

            assertEquals(expected.getFullName() + " field " + expField.name(),
                         expField.order(),
                         actualField.order());

            assertEquals(expected.getFullName() + " field " + expField.name(),
                         expField.defaultValue(),
                         actualField.defaultValue());

            assertEquivalent(expField.schema(),
                             actualField.schema(),
                             recordsSeen);
          }
        }
        break;
      }
    default:
      // All primitive types are equal if their types are.
    }
  }

  static File buildFile(String... parts) {
    File file = null;

    for (String part : parts) {
      if (file == null) {
        file = new File(part);
      } else {
        file = new File(file, part);
      }
    }

    return file;
  }
}
