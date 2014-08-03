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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

public class XmlDatumConfig {

	private XmlDatumConfig(QName rootTagName) {
		baseTagName = rootTagName;
		schemaUrls = null;
		schemaFiles = null;
		baseUri = null;
	}

	public XmlDatumConfig(URL schema, QName rootTagName) {
		this(rootTagName);
		schemaUrls = new ArrayList<URL>(1);
		schemaUrls.add(schema);
		baseUri = getBaseUriFor(schema);
	}

	public XmlDatumConfig(File schema, String schemaBaseUri, QName rootTagName) {
		this(rootTagName);

		schemaFiles = new ArrayList<File>(1);
		schemaFiles.add(schema);
		baseUri = schemaBaseUri;
	}

	public String getBaseUri() {
		return baseUri;
	}

	public List<String> getSchemaPaths() {
	  final ArrayList<String> paths = new ArrayList<String>( getArraySize() );
		if (schemaUrls != null) {
			for (URL schemaUrl : schemaUrls) {
			  paths.add( schemaUrl.toString() );
			}
		}
		if (schemaFiles != null) {
		  for (File schemaFile : schemaFiles) {
		    paths.add( schemaFile.getAbsolutePath() );
		  }
		}
		return paths;
	}

	public QName getRootTagName() {
		return baseTagName;
	}

	public void addSchemaUrl(URL schemaUrl) {
	  if (schemaUrls == null) {
	    schemaUrls = new ArrayList<URL>(1);
	  }
	  schemaUrls.add(schemaUrl);
	}

	public void addSchemaFile(File file) {
	  if (schemaFiles == null) {
	    schemaFiles = new ArrayList<File>(1);
	  }
	  schemaFiles.add(file);
	}

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
              schemaFile.getAbsolutePath());
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

  private ArrayList<URL> schemaUrls;
	private ArrayList<File> schemaFiles;
	private String baseUri;
	private QName baseTagName;
}