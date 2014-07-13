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
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.w3c.dom.Document;

/**
 * Reads an XML {@link Document} from a {@link Decoder}.
 * If the {@link Schema} can be conformed to, it will be.
 *
 * @author  Mike Pigott (mpigott.subscriptions@gmail.com)
 */
public class XmlDatumReader implements DatumReader<Document> {

    /**
     * Creates an {@link XmlDatumReader} with the {@link XmlSchemaCollection}
     * to use when decoding XML {@link Document}s from {@link Decoder}s.
     */
    public XmlDatumReader(XmlSchemaCollection xmlSchemaCollection) {
	this.xmlSchemaCollection = xmlSchemaCollection;
    }

    /**
     * Sets the {@link Schema} that defines how data will be read from the
     * {@link Decoder} when {@link #read(Document, Decoder)} is called.
     *
     * <p>
     * Checks the input {@link Schema} conforms with the provided
     * {@link XmlSchemaCollection}.  If <code>schema</code> does not conform,
     * an {@link IllegalArgumentException} is thrown.
     * </p>
     *
     * @throws IllegalArgumentException if the schema is <code>null</code> or
     *                                  does not conform to the corresponding
     *                                  XML schema.
     *
     * @see org.apache.avro.io.DatumReader#setSchema(org.apache.avro.Schema)
     */
    @Override
    public void setSchema(Schema schema) {
	if (schema == null) {
	    throw new IllegalArgumentException("Input schema cannot be null.");
	}

	if (xmlSchemaCollection != null) {
	    // TODO: Confirm the schema validates against xmlSchemaCollection.
	}

	inputSchema = schema;
    }

    /**
     * Reads the XML {@link Document} from the input {@link Decoder} and
     * returns it, transformed.  The <code>reuse</code> {@link Document}
     * will not be used, as {@link Document} re-use is difficult.
     *
     * @see org.apache.avro.io.DatumReader#read(java.lang.Object, org.apache.avro.io.Decoder)
     */
    @Override
    public Document read(Document reuse, Decoder in) throws IOException {
	return null;
    }

    private Schema inputSchema;
    private XmlSchemaCollection xmlSchemaCollection;
}
