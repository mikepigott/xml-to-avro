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

import java.net.URISyntaxException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;

/**
 * A set of utilities for encoding and
 * decoding XML Documents and Avro data.
 *
 * @author  Mike Pigott (mpigott.subscriptions@gmail.com)
 */
class Utils {

    static String getAvroNamespaceFor(String xmlSchemaNamespace) throws URISyntaxException {
	return getAvroNamespaceFor(new URI(xmlSchemaNamespace));
    }

    static String getAvroNamespaceFor(URL xmlSchemaNamespace) throws URISyntaxException {
	return getAvroNamespaceFor( xmlSchemaNamespace.toURI() );
    }

    static String getAvroNamespaceFor(java.net.URI uri) {
	ArrayList<String> components = new ArrayList<String>();

	// xsd.example.org -> org.example.xsd
	final String host = uri.getHost();
	if (host != null) {
	    String[] hostParts = host.split("\\.");
	    for (int hpIdx = hostParts.length - 1; hpIdx >= 0; --hpIdx) {
		if ( !hostParts[hpIdx].isEmpty() ) {
		    components.add(hostParts[hpIdx]);
		}
	    }
	}

	// path/to/schema.xsd -> path.to.schema.xsd
	final String path = uri.getPath();
	if (path != null) {
	    String[] pathParts = path.split("/");
	    for (String pathPart : pathParts) {
		if ( !pathPart.isEmpty() ) {
		    components.add(pathPart);
		}
	    }
	}

	if ( components.isEmpty() ) {
	    throw new IllegalArgumentException("URI provided without enough content to create a namespace for.");
	}

	StringBuilder namespace = new StringBuilder(components.get(0));
	for (int c = 1; c < components.size(); ++c) {
	    namespace.append('.').append( components.get(c) );
	}

	return namespace.toString();
    }
}
