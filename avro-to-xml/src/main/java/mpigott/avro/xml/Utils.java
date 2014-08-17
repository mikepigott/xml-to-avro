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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.constants.Constants;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.xml.sax.InputSource;

/**
 * A set of utilities for encoding and
 * decoding XML Documents and Avro data.
 */
class Utils {

  private static final int UNDERSCORE_CP = '_';
  private static final int PERIOD_CP = '.';

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
    xmlToAvroTypeMap.put(Constants.XSD_UNSIGNEDLONG,  Schema.Type.DOUBLE);
    xmlToAvroTypeMap.put(Constants.XSD_QNAME,         Schema.Type.RECORD);
  }

  static Set<QName> getAvroRecognizedTypes() {
    return xmlToAvroTypeMap.keySet();
  }

  static Schema.Type getAvroSchemaTypeFor(QName qName) {
    return xmlToAvroTypeMap.get(qName);
  }

  static Schema getAvroSchemaFor(
      XmlSchemaTypeInfo typeInfo,
      QName qName,
      boolean isOptional) {

    switch ( typeInfo.getType() ) {
    case ATOMIC:
      {
        Schema schema = null;
        if ( isValidEnum(typeInfo) ) {
          // This is an enumeration!
          final HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>>
            facets = typeInfo.getFacets();

          String ns = null;
          try {
            ns = Utils.getAvroNamespaceFor(qName.getNamespaceURI()) + ".enums";
          } catch (URISyntaxException e) {
            throw new IllegalArgumentException(
                qName + " does not have a valid namespace.", e);
          }

          final List<XmlSchemaRestriction> enumFacet =
              facets.get(XmlSchemaRestriction.Type.ENUMERATION);

          final ArrayList<String> symbols =
              new ArrayList<String>( enumFacet.size() );
          for (XmlSchemaRestriction enumSym : enumFacet) {
            symbols.add( enumSym.getValue().toString() );
          }

          schema =
              Schema.createEnum(
                  qName.getLocalPart(),
                  "Enumeration of symbols in " + qName,
                  ns,
                  symbols);

        } else if (
            typeInfo.getBaseType().equals(XmlSchemaBaseSimpleType.QNAME)) {

          /* QNames will be represented as a RECORD
           * with a namespace and a local part.
           */
          final List<Schema.Field> fields = new ArrayList<Schema.Field>(2);
          fields.add(
              new Schema.Field(
                  "namespace",
                  Schema.create(Schema.Type.STRING),
                  "The namespace of this qualified name.",
                  null) );

          fields.add(
              new Schema.Field(
                  "localPart",
                  Schema.create(Schema.Type.STRING),
                  "The local part of this qualified name.",
                  null) );

          String ns = null;
          try {
            ns = Utils.getAvroNamespaceFor( Constants.URI_2001_SCHEMA_XSD );
          } catch (URISyntaxException e) {
            throw new IllegalStateException(
                "Cannot create an Avro namespace for "
                + Constants.URI_2001_SCHEMA_XSD);
          }

          schema =
              Schema.createRecord("qName", "Qualified Name", ns, false);
          schema.setFields(fields);

        } else {

          final Schema.Type avroType =
            xmlToAvroTypeMap.get( typeInfo.getUserRecognizedType() );
  
          if (avroType == null) {
            throw new IllegalArgumentException(
                "No Avro type recognized for "
                + typeInfo.getUserRecognizedType());
          }
  
          schema = Schema.create(avroType);
        }

        return createSchemaOf(schema, isOptional, typeInfo.isMixed());
      }
    case LIST:
      {
        Schema schema =
            Schema.createArray(
                getAvroSchemaFor(
                    typeInfo.getChildTypes().get(0), qName, false) );

        return createSchemaOf(schema, isOptional, typeInfo.isMixed());
      }
    case UNION:
      {
        List<XmlSchemaTypeInfo> unionTypes = typeInfo.getChildTypes();
        List<Schema> avroTypes =
            new ArrayList<Schema>(unionTypes.size() + 2);

        for (XmlSchemaTypeInfo unionType : unionTypes) {
          final Schema avroSchema = getAvroSchemaFor(unionType, qName, false);
          if ( !avroTypes.contains(avroSchema) ) {
            avroTypes.add(avroSchema);
          }
        }

        if (isOptional) {
          final Schema avroSchema = Schema.create(Schema.Type.NULL);
          if ( !avroTypes.contains(avroSchema) ) {
            avroTypes.add(avroSchema);
          }
        }
        if ( typeInfo.isMixed() ) {
          final Schema avroSchema = Schema.create(Schema.Type.STRING);
          if ( !avroTypes.contains(avroSchema) ) {
            avroTypes.add(avroSchema);
          }
        }

        return Schema.createUnion(avroTypes);
      }
    case COMPLEX:
      {
        if ( typeInfo.isMixed() ) {
          return Schema.create(Schema.Type.STRING);
        }
        /* falls through */
      }
    default:
      throw new IllegalArgumentException(
          "Cannot create an Avro schema for a "
          + typeInfo.getType()
          + " type.");
    }
  }

  private static Schema createSchemaOf(
      Schema schema,
      boolean isOptional,
      boolean isMixed) {

    if (!isOptional && !isMixed) {
      return schema;
    }
    List<Schema> unionTypes = new ArrayList<Schema>(2);
    unionTypes.add(schema);
    if (isOptional) {
      unionTypes.add( Schema.create(Schema.Type.NULL) );
    }
    if (isMixed) {
      unionTypes.add( Schema.create(Schema.Type.STRING) );
    }
    schema = Schema.createUnion(unionTypes);
    return schema;
  }

  private static boolean isValidEnum(XmlSchemaTypeInfo typeInfo) {
    final HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>>
      facets = typeInfo.getFacets();

    if (facets == null) {
      return false;
    }

    final List<XmlSchemaRestriction> enumFacets =
        facets.get(XmlSchemaRestriction.Type.ENUMERATION);

    if (enumFacets == null) {
      return false;
    }

    for (XmlSchemaRestriction enumFacet : enumFacets) {
      final String symbol = enumFacet.getValue().toString();

      final int length = symbol.length();
      for (int offset = 0; offset < length; ) {
        final int codepoint = symbol.codePointAt(offset);
        if (!Character.isLetterOrDigit(codepoint)
            && (codepoint != UNDERSCORE_CP)) {
          return false;
        }

        offset += Character.charCount(codepoint);
      }
    }

    return true;
  }

  static XmlSchemaTypeInfo chooseUnionType(
      XmlSchemaTypeInfo xmlType,
      QName typeQName,
      Schema elemType,
      int unionIndex) {

    XmlSchemaTypeInfo xmlElemType = xmlType;
    if (xmlType.getChildTypes().size() <= unionIndex) {
      xmlElemType = null;
    } else {
      for (XmlSchemaTypeInfo childType : xmlType.getChildTypes()) {
        final Schema avroSchemaOfChildType =
            Utils.getAvroSchemaFor(childType, typeQName, false);
        if ( avroSchemaOfChildType.equals(elemType) ) {
          xmlElemType = childType;
          break;
        }
      }
    }
    return xmlElemType;

  }

  static String getAvroNamespaceFor(String xmlSchemaNamespace)
      throws URISyntaxException {

    return getAvroNamespaceFor(new URI(xmlSchemaNamespace));
  }

  static String getAvroNamespaceFor(URL xmlSchemaNamespace)
      throws URISyntaxException {

    return getAvroNamespaceFor( xmlSchemaNamespace.toURI() );
  }

  static String getAvroNamespaceFor(java.net.URI uri) {
    final ArrayList<String> components = new ArrayList<String>();

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
	    final String[] pathParts = path.split("/");
	    for (String pathPart : pathParts) {
		    if ( !pathPart.isEmpty() ) {
		      components.add(pathPart);
		    }
	    }
	  }

	  /* This URI is of the form a:b:c:d:e.
	   * We can convert that to a.b.c.d.e.
	   */
	  if ( components.isEmpty() ) {
	    final String schemeSpecificPart = uri.getSchemeSpecificPart();
	    final String[] schemeParts = schemeSpecificPart.split("\\:");

	    for (String schemePart : schemeParts) {
	      if ( !schemePart.isEmpty() ) {
	        components.add(schemePart);
	      }
	    }
	  }

	  if ( components.isEmpty() ) {
	    throw new IllegalArgumentException(
	        "URI \"" + uri.toString()
	        + "\" does not have enough content to create a namespace for it.");
	  }

	  StringBuilder namespace = new StringBuilder(components.get(0));
	  for (int c = 1; c < components.size(); ++c) {
	    namespace.append('.').append( createValidName( components.get(c) ) );
	  }

	  return namespace.toString();
  }

  private static String createValidName(String component) {
    StringBuilder str = new StringBuilder();
    final int length = component.length();
    for (int offset = 0; offset < length; ) {
      final int codepoint = component.codePointAt(offset);
      if (!Character.isLetterOrDigit(codepoint)
          && (codepoint != UNDERSCORE_CP)
          && (codepoint != PERIOD_CP)) {
        str.append('_');
      } else {
        str.append( Character.toChars(codepoint) );
      }

      offset += Character.charCount(codepoint);
    }
    return str.toString();
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
      if (value.equalsIgnoreCase("true")
          || value.equalsIgnoreCase("false") ) {

        return JsonNodeFactory
                 .instance
                 .booleanNode( Boolean.parseBoolean(value) );
      }
      break;

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

    case RECORD:
      {
        /* TODO: Can only be converted to QName or Decimal; will need a
         *       NamespaceContext and a check if value is numeric or not.
         */
      }

    default:
    }

    throw new IllegalArgumentException(
        "Could not parse the value \""
        + value
        + "\" using the provided schema "
        + type);
  }
}
