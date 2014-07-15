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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

/**
 * Reads an XML {@link Document} and writes it to an {@link Encoder}.
 *
 * @author  Mike Pigott
 */
public class XmlDatumWriter implements DatumWriter<Document> {

  public XmlDatumWriter() {
    xmlSchemaCollection = null;
    schema = null;
  }

  public XmlDatumWriter(XmlSchemaCollection xmlSchemaCollection) {
    this.xmlSchemaCollection = xmlSchemaCollection;
    this.schema = null;
  }

  /**
   * Sets the schema to use when writing the XML
   * {@link Document} to the {@link Encoder}.
   *
   * <p>
   * If the {@link Schema} is <code>null</code>, generates one on the fly
   * from the document itself.  That {@link Schema can be retrieved by
   * calling {@link #getSchema()}.
   * </p>
   *
   * @see org.apache.avro.io.DatumWriter#setSchema(org.apache.avro.Schema)
   */
  @Override
  public void setSchema(Schema schema) {
    if (xmlSchemaCollection != null) {
      // TODO: Validate against the XML schema.
    }

    this.schema = schema;
  }

  /**
   * Writes the {@link Document} to the {@link Encoder} in accordance
   * with the {@link Schema} set in {@link #setSchema(Schema)}.
   *
   * <p>
   * If no {@link Schema} was provided, builds one from the {@link Document}
   * and its {@link XmlSchemaCollection}.  The schema can then be retrieved
   * from {@link #getSchema()}.
   * </p>
   *
   * @see org.apache.avro.io.DatumWriter#write(java.lang.Object, org.apache.avro.io.Encoder)
   */
  @Override
  public void write(Document doc, Encoder out) throws IOException {
  }

  private XmlSchemaCollection xmlSchemaCollection;
  private Schema schema;
}
