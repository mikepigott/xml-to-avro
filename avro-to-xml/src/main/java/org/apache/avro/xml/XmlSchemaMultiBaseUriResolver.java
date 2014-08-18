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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.ListIterator;

import org.apache.ws.commons.schema.resolver.DefaultURIResolver;
import org.xml.sax.InputSource;

/**
 * This class is used by
 * {@link org.apache.ws.commons.schema.XmlSchemaCollection}
 * to resolve schemas from multiple base URIs.
 */
class XmlSchemaMultiBaseUriResolver extends DefaultURIResolver {

  private List<String> baseUris;

  public XmlSchemaMultiBaseUriResolver() {
    baseUris = new java.util.ArrayList<String>();
  }

  /**
   * Resolves the schema at the provided location
   * with the specified input namespace and base URI.
   *
   * @see org.apache.ws.commons.schema.resolver.URIResolver#resolveEntity(String, String, String)
   */
  @SuppressWarnings("unused")
  public InputSource resolveEntity(
      String namespace,
      String schemaLocation,
      String baseUri) {

    InputSource source = null;
    if ((baseUri != null) && !baseUri.isEmpty()) {
      baseUris.add(baseUri);
    }

    /* Confirm the schema location is a fully-qualified
     * path before adding it to the set of base URIs.
     */
    try {
      new URL(schemaLocation);
      baseUris.add(schemaLocation);
    } catch (MalformedURLException e) {
    }

    /* When we receive a schema location, it may only be a partial path.
     * That partial path may come from one of many different base URIs
     * that we've seen already, most likely from one we recently tried.
     * So, in order to determine which base URI the partial schema comes
     * from, we must try them all and see which one resolves.
     *
     * We check in reverse order because a schema is likely tied to a
     * recent base URI we have already seen.
     */
    ListIterator<String> iter = baseUris.listIterator(baseUris.size() - 1);
    while (iter.hasPrevious()) {
      try {
        String newBaseUri = iter.previous();
        source = super.resolveEntity(namespace, schemaLocation, newBaseUri);
        InputStream urlStream = null;
        try {
          urlStream = new URL(source.getSystemId()).openStream();
        } finally {
          if (urlStream != null) {
         	  try {
              urlStream.close();
            } catch (IOException ioe) {
              // No error for failure to close.
            }
          }
        }
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
  @Override
  public String getCollectionBaseURI() {
    return baseUris.isEmpty() ? null : baseUris.get(0);
  }

  /**
   * Adds the provided URI to the set of base URIs to check.
   */
  @Override
  public void setCollectionBaseURI(String uri) {
    baseUris.add(uri);
  }
}
