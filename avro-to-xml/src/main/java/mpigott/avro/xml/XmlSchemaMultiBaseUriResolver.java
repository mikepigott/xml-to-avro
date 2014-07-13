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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.ListIterator;

import org.apache.ws.commons.schema.resolver.DefaultURIResolver;
import org.xml.sax.InputSource;

/**
 * This class is used by {@link org.apache.ws.commons.schema.XmlSchemaCollection}
 * to resolve schemas from multiple base URIs.
 *
 * @author  Mike Pigott (mpigott.subscriptions@gmail.com)
 * @version 1.0
 */
class XmlSchemaMultiBaseUriResolver extends DefaultURIResolver {

    public XmlSchemaMultiBaseUriResolver() {
        baseUris = new java.util.ArrayList<String>();
    }

    /**
     * Resolves the schema at the provided location
     * with the specified input namespace and base URI.
     *
     * @see org.apache.ws.commons.schema.resolver.URIResolver#resolveEntity(String, String, String)
     */
    public InputSource resolveEntity(String namespace, String schemaLocation, String baseUri) {
        InputSource source = null;
        if ((baseUri != null) && !baseUri.isEmpty()) {
            baseUris.add(baseUri);
        }

        // Confirm the schema location is valid before adding it.
        try {
            new URL(schemaLocation);
            baseUris.add(schemaLocation);
        } catch (MalformedURLException e) {
        }

        ListIterator<String> iter = baseUris.listIterator(baseUris.size() - 1);
        while (iter.hasPrevious()) {
            try {
                String newBaseUri = iter.previous();
                source = super.resolveEntity(namespace, schemaLocation, newBaseUri);
                new URL(source.getSystemId()).openStream();
                break;
            } catch (IOException ioe) {
                /* If we reach here, we were unable to open a
                 * connection to the source.  Try the next one.
                 */
            }
        }
        return source;
    }

    /**
     * Returns one of the base URIs provided earlier.
     */
    public String getCollectionBaseURI() {
        return baseUris.isEmpty() ? null : baseUris.get(0);
    }

    /**
     * Adds the provided URI to the set of base URIs to check.
     */
    public void setCollectionBaseURI(String uri) {
        baseUris.add(uri);
    }

    private List<String> baseUris;
}
