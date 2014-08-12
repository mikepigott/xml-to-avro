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

import org.apache.ws.commons.schema.constants.Constants;
import org.junit.Test;

/**
 * Tests the {@link XmlSchemaBaseSimpleType}.
 *
 * @author  Mike Pigott
 */
public class TestXmlSchemaBaseSimpleType {

  @Test
  public void testMappings() {
    assertEquals(Constants.XSD_ANYTYPE,
                 XmlSchemaBaseSimpleType.ANYTYPE.getQName());

    assertEquals(Constants.XSD_ANYSIMPLETYPE,
        XmlSchemaBaseSimpleType.ANYSIMPLETYPE.getQName());

    assertEquals(Constants.XSD_DURATION,
        XmlSchemaBaseSimpleType.DURATION.getQName());

    assertEquals(Constants.XSD_DATETIME,
        XmlSchemaBaseSimpleType.DATETIME.getQName());

    assertEquals(Constants.XSD_TIME,
        XmlSchemaBaseSimpleType.TIME.getQName());

    assertEquals(Constants.XSD_DATE,
        XmlSchemaBaseSimpleType.DATE.getQName());

    assertEquals(Constants.XSD_YEARMONTH,
        XmlSchemaBaseSimpleType.YEARMONTH.getQName());

    assertEquals(Constants.XSD_YEAR,
        XmlSchemaBaseSimpleType.YEAR.getQName());

    assertEquals(Constants.XSD_MONTHDAY,
        XmlSchemaBaseSimpleType.MONTHDAY.getQName());

    assertEquals(Constants.XSD_DAY,
        XmlSchemaBaseSimpleType.DAY.getQName());

    assertEquals(Constants.XSD_MONTH,
        XmlSchemaBaseSimpleType.MONTH.getQName());

    assertEquals(Constants.XSD_STRING,
        XmlSchemaBaseSimpleType.STRING.getQName());

    assertEquals(Constants.XSD_BOOLEAN,
        XmlSchemaBaseSimpleType.BOOLEAN.getQName());

    assertEquals(Constants.XSD_BASE64,
        XmlSchemaBaseSimpleType.BIN_BASE64.getQName());

    assertEquals(Constants.XSD_HEXBIN,
        XmlSchemaBaseSimpleType.BIN_HEX.getQName());

    assertEquals(Constants.XSD_FLOAT,
        XmlSchemaBaseSimpleType.FLOAT.getQName());

    assertEquals(Constants.XSD_DECIMAL,
        XmlSchemaBaseSimpleType.DECIMAL.getQName());

    assertEquals(Constants.XSD_DOUBLE,
        XmlSchemaBaseSimpleType.DOUBLE.getQName());

    assertEquals(Constants.XSD_ANYURI,
        XmlSchemaBaseSimpleType.ANYURI.getQName());

    assertEquals(Constants.XSD_QNAME,
        XmlSchemaBaseSimpleType.QNAME.getQName());

    assertEquals(Constants.XSD_NOTATION,
        XmlSchemaBaseSimpleType.NOTATION.getQName());
  }

  @Test
  public void testReverseMappings() {
    assertEquals(
        XmlSchemaBaseSimpleType.ANYTYPE,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_ANYTYPE));

    assertEquals(
        XmlSchemaBaseSimpleType.ANYSIMPLETYPE,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(
            Constants.XSD_ANYSIMPLETYPE));

    assertEquals(
        XmlSchemaBaseSimpleType.DURATION,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_DURATION));

    assertEquals(
        XmlSchemaBaseSimpleType.DATETIME,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_DATETIME));

    assertEquals(
        XmlSchemaBaseSimpleType.TIME,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_TIME));

    assertEquals(
        XmlSchemaBaseSimpleType.DATE,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_DATE));

    assertEquals(
        XmlSchemaBaseSimpleType.YEARMONTH,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_YEARMONTH));

    assertEquals(
        XmlSchemaBaseSimpleType.YEAR,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_YEAR));

    assertEquals(
        XmlSchemaBaseSimpleType.MONTHDAY,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_MONTHDAY));

    assertEquals(
        XmlSchemaBaseSimpleType.DAY,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_DAY));

    assertEquals(
        XmlSchemaBaseSimpleType.MONTH,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_MONTH));

    assertEquals(
        XmlSchemaBaseSimpleType.STRING,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_STRING));

    assertEquals(
        XmlSchemaBaseSimpleType.BOOLEAN,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_BOOLEAN));

    assertEquals(
        XmlSchemaBaseSimpleType.BIN_BASE64,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_BASE64));

    assertEquals(
        XmlSchemaBaseSimpleType.BIN_HEX,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_HEXBIN));

    assertEquals(
        XmlSchemaBaseSimpleType.FLOAT,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_FLOAT));

    assertEquals(
        XmlSchemaBaseSimpleType.DECIMAL,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_DECIMAL));

    assertEquals(
        XmlSchemaBaseSimpleType.DOUBLE,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_DOUBLE));

    assertEquals(
        XmlSchemaBaseSimpleType.ANYURI,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_ANYURI));

    assertEquals(
        XmlSchemaBaseSimpleType.QNAME,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_QNAME));

    assertEquals(
        XmlSchemaBaseSimpleType.NOTATION,
        XmlSchemaBaseSimpleType.getBaseSimpleTypeFor(Constants.XSD_NOTATION));
  }
}
