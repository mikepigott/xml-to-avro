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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.constants.Constants;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.xml.sax.InputSource;

/**
 * A set of utilities for encoding and
 * decoding XML Documents and Avro data.
 *
 * @author  Mike Pigott
 */
class Utils {

  private static final Map<QName, Schema.Type> xmlToAvroTypeMap =
      new HashMap<QName, Schema.Type>();

  static {
    xmlToAvroTypeMap.put(Constants.XSD_ANYTYPE,       Schema.Type.STRING);
    xmlToAvroTypeMap.put(Constants.XSD_BOOLEAN,       Schema.Type.BOOLEAN);
    xmlToAvroTypeMap.put(Constants.XSD_DECIMAL,       Schema.Type.DOUBLE);
    xmlToAvroTypeMap.put(Constants.XSD_DOUBLE,        Schema.Type.DOUBLE);
    xmlToAvroTypeMap.put(Constants.XSD_FLOAT,         Schema.Type.FLOAT);
    xmlToAvroTypeMap.put(Constants.XSD_BASE64,        Schema.Type.BYTES);
    xmlToAvroTypeMap.put(Constants.XSD_HEXBIN,        Schema.Type.BYTES);
    xmlToAvroTypeMap.put(Constants.XSD_LONG,          Schema.Type.LONG);
    xmlToAvroTypeMap.put(Constants.XSD_ID,            Schema.Type.STRING);
    xmlToAvroTypeMap.put(Constants.XSD_INT,           Schema.Type.INT);
    xmlToAvroTypeMap.put(Constants.XSD_UNSIGNEDINT,   Schema.Type.LONG);
    xmlToAvroTypeMap.put(Constants.XSD_UNSIGNEDSHORT, Schema.Type.INT);
  }

  static Set<QName> getAvroRecognizedTypes() {
    return xmlToAvroTypeMap.keySet();
  }

  static Schema.Type getAvroSchemaTypeFor(QName qName) {
    return xmlToAvroTypeMap.get(qName);
  }

  static Schema getAvroSchemaFor(
      XmlSchemaTypeInfo typeInfo,
      boolean isOptional) {

    switch ( typeInfo.getType() ) {
    case ATOMIC:
      {
        Schema.Type avroType =
          xmlToAvroTypeMap.get( typeInfo.getUserRecognizedType() );

        if (avroType == null) {
          throw new IllegalArgumentException("No Avro type recognized for " + typeInfo.getUserRecognizedType());
        }

        Schema schema = Schema.create(avroType);

        if (isOptional) {
          schema = createOptionalTypeOf(schema);
        }

        return schema;
      }
    case LIST:
      {
        Schema schema =
            Schema.createArray(
                getAvroSchemaFor(
                    typeInfo.getChildTypes().get(0), false) );

        if (isOptional) {
          schema = createOptionalTypeOf(schema);
        }

        return schema;
      }
    case UNION:
      List<XmlSchemaTypeInfo> unionTypes = typeInfo.getChildTypes();
      List<Schema> avroTypes = new ArrayList<Schema>( unionTypes.size() );

      for (XmlSchemaTypeInfo unionType : unionTypes) {
        avroTypes.add( getAvroSchemaFor(unionType, false) );
      }

      if (isOptional) {
        avroTypes.add( Schema.create(Schema.Type.NULL) );
      }

      return Schema.createUnion(avroTypes);
    default:
      throw new IllegalArgumentException("Cannot create an Avro schema for a " + typeInfo.getType() + " type.");
    }
  }

  private static Schema createOptionalTypeOf(Schema schema) {
    List<Schema> unionTypes = new ArrayList<Schema>(2);
    unionTypes.add(schema);
    unionTypes.add( Schema.create(Schema.Type.NULL) );
    return Schema.createUnion(unionTypes);
  }

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

  static InputSource getSchema(String docBaseUri, String schemaLocation) throws java.io.IOException {
	  URL schemaUrl = null;
	  if (schemaLocation.contains("://")) {
	    schemaUrl = new URL(schemaLocation);
	  } else if (docBaseUri.endsWith("/")) {
	    schemaUrl = new URL(docBaseUri + schemaLocation);
	  } else {
	    schemaUrl = new URL(docBaseUri + "/" + schemaLocation);
	  }
	  return new InputSource( schemaUrl.openStream() );
  }

  static JsonNode createJsonNodeFor(String value, Schema type) {
    if ((value == null) || value.isEmpty()) {
      return null;
    }

    switch ( type.getType() ) {
    case ARRAY:
      {
        final Schema subType = type.getElementType();
        final String[] elems = value.split(" ");
        final ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        for (String elem : elems) {
          arrayNode.add( createJsonNodeFor(elem, subType) );
        }
        return arrayNode;
      }
    case UNION:
      {
        final List<Schema> subTypes = type.getTypes();
        Schema textType = null;
        for (Schema subType : subTypes) {
          // Try the text types last.
          if (subType.getType().equals(Schema.Type.BYTES)
              || subType.getType().equals(Schema.Type.STRING)) {
            textType = subType;
            continue;
          }

          try {
            return createJsonNodeFor(value, subType);
          } catch (Exception e) {
            /* Could not parse the value using the
             * provided type; try the next one.
             */
          }
        }

        if (textType != null) {
          return createJsonNodeFor(value, textType);
        }
        break;
      }
    case BOOLEAN:
      return JsonNodeFactory.instance.booleanNode( Boolean.parseBoolean(value) );

    case BYTES:
    case STRING:
      return JsonNodeFactory.instance.textNode(value);

    case DOUBLE:
    case FLOAT:
      return JsonNodeFactory.instance.numberNode( Double.parseDouble(value) );

    case INT:
      return JsonNodeFactory.instance.numberNode( Integer.parseInt(value) );

    case LONG:
      return JsonNodeFactory.instance.numberNode( Long.parseLong(value) );

    default:
    }

    throw new IllegalArgumentException("Could not parse the value \"" + value + "\" using the provided schema " + type);
  }
}
