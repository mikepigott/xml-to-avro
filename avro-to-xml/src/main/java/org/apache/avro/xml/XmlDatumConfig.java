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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

/**
 * Configuration for setting up the {@link XmlDatumWriter}.  XML Schemas may
 * be defined by {@link java.net.URL}s and/or {@link java.io.File}s, and the
 * name of the XML Document's root tag {@link javax.xml.namespace.QName} is
 * needed to determine how to parse the XML Schema or Schemas.
 *
 * <p>
 * When using files, a Schema URI is required.  If the Schema contains
 * references to incomplete paths, this URI will be used to track them
 * down.
 * </p>
 */
public class XmlDatumConfig {

  private ArrayList<URL> schemaUrls;
  private ArrayList<File> schemaFiles;
  private String baseUri;
  private QName baseTagName;

  private XmlDatumConfig(QName rootTagName) {
    baseTagName = rootTagName;
    schemaUrls = null;
    schemaFiles = null;
    baseUri = null;
  }

  /**
   * Creates a new <code>XmlDatumConfig</code> from the {@link java.net.URL}
   * to fetch the XML Schema from and the {@link QName} representing the
   * root element.
   *
   * @param schema      The URL of the XML Schema to read.
   * @param rootTagName The <code>QName</code> of the root element.
   */
  public XmlDatumConfig(URL schema, QName rootTagName) {
    this(rootTagName);
    schemaUrls = new ArrayList<URL>(1);
    schemaUrls.add(schema);
    baseUri = getBaseUriFor(schema);
  }

  /**
   * Creates a new <code>XmlDatumConfig</code> from a local {@link File}
   * to read the XML Schema from and the {@link QName} representing the
   * root element.  A base URI is also required as the default path to
   * fetch any referenced schemas from.
   *
   * @param schema        The path to the XML Schema to read.
   *
   * @param schemaBaseUri The base URI of the schema - a default URI of
   *                      where to retrieve other referenced schemas.
   *
   * @param rootTagName   The <code>QName</code> of the root element.
   */
  public XmlDatumConfig(File schema, String schemaBaseUri, QName rootTagName) {
    this(rootTagName);

    schemaFiles = new ArrayList<File>(1);
    schemaFiles.add(schema);
    baseUri = schemaBaseUri;
  }

  /**
   * <p>
   * The base URI.  If this <code>XmlDatumConfig</code> was created with a
   * URL, the base URI is the URL up to, but not including the schema file
   * name.
   * </p>
   * <p>
   * If the <code>XmlDatumConfig</code> was created with a <code>File</code>,
   * this is the <code>schemaBaseUri</code> provided in that constructor.
   * </p>
   */
  public String getBaseUri() {
    return baseUri;
  }

  /**
   * The list of XML Schema URLs provided via
   * {@link #XmlDatumConfig(URL, QName)} and
   * any subsequent calls to
   * {@link #addSchemaUrl(URL)}.
   */
  public List<URL> getSchemaUrls() {
    return schemaUrls;
  }

  /**
   * The list of XML Schema files provided via
   * {@link #XmlDatumConfig(File, String, QName)}
   * and any subsequent calls to
   * {@link #addSchemaFile(File)}.
   */
  public List<File> getSchemaFiles() {
    return schemaFiles;
  }

  /**
   * The XML Schema's root tag passed into the constructor.
   */
  public QName getRootTagName() {
    return baseTagName;
  }

  /**
   * Adds a URL to an XML Schema to include when generating Avro.
   */
  public void addSchemaUrl(URL schemaUrl) {
    if (schemaUrls == null) {
      schemaUrls = new ArrayList<URL>(1);
    }
    schemaUrls.add(schemaUrl);
  }

  /**
   * Adds a file path to an XML Schema to include when generating Avro.
   */
  public void addSchemaFile(File file) {
    if (schemaFiles == null) {
      schemaFiles = new ArrayList<File>(1);
    }
    schemaFiles.add(file);
  }

  @SuppressWarnings("resource")
  List<StreamSource> getSources() throws IOException {
    final ArrayList<StreamSource> sources =
        new ArrayList<StreamSource>( getArraySize() );
    try {
      if (schemaUrls != null) {
        for (URL schemaUrl : schemaUrls) {
          final StreamSource source =
            new StreamSource(
              schemaUrl.openStream(),
              schemaUrl.toString());
          sources.add(source);
        }
      }
      if (schemaFiles != null) {
        for (File schemaFile : schemaFiles) {
          final StreamSource source =
            new StreamSource(
              new FileReader(schemaFile),
              schemaFile.getName());
          sources.add(source);
        }
      }
    } catch (IOException ioe) {
      for (StreamSource source : sources) {
        try {
          if (source.getInputStream() != null) {
            source.getInputStream().close();
          } else {
            source.getReader().close();
          }
        } catch (IOException failedClose) {
          failedClose.printStackTrace();
        }
      }
      throw ioe;
    }
    return sources;
  }

  private int getArraySize() {
    int count = 0;
    if (schemaUrls != null) {
      count += schemaUrls.size();
    }
    if (schemaFiles != null) {
      count += schemaFiles.size();
    }
    return count;
  }

  private static String getBaseUriFor(URL url) {
    StringBuilder namespace = new StringBuilder( url.getProtocol() );
    namespace.append("://").append( url.getHost() );
    if (url.getPort() != -1) {
      namespace.append(':').append( url.getPort() );
    }
    namespace.append( url.getPath() );

    String namespaceWithFile = namespace.toString();
    int lastSlashBeforeFileIndex = namespaceWithFile.lastIndexOf('/');

    return namespaceWithFile.substring(0, lastSlashBeforeFileIndex + 1);
  }
}
