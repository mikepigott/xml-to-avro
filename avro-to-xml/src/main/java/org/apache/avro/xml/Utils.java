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
package org.apache.avro.xml;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
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
import org.codehaus.jackson.node.IntNode;
import org.codehaus.jackson.node.NumericNode;
import org.codehaus.jackson.node.TextNode;

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
    xmlToAvroTypeMap.put(Constants.XSD_DECIMAL,       Schema.Type.BYTES);
    xmlToAvroTypeMap.put(Constants.XSD_DOUBLE,        Schema.Type.DOUBLE);
    xmlToAvroTypeMap.put(Constants.XSD_FLOAT,         Schema.Type.FLOAT);
    xmlToAvroTypeMap.put(Constants.XSD_BASE64,        Schema.Type.BYTES);
    xmlToAvroTypeMap.put(Constants.XSD_HEXBIN,        Schema.Type.BYTES);
    xmlToAvroTypeMap.put(Constants.XSD_LONG,          Schema.Type.LONG);
    xmlToAvroTypeMap.put(Constants.XSD_ID,            Schema.Type.STRING);
    xmlToAvroTypeMap.put(Constants.XSD_INT,           Schema.Type.INT);
    xmlToAvroTypeMap.put(Constants.XSD_UNSIGNEDINT,   Schema.Type.LONG);
    xmlToAvroTypeMap.put(Constants.XSD_UNSIGNEDSHORT, Schema.Type.INT);
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

          // DECIMAL is a logical type.
          if (schema.getType().equals(Schema.Type.BYTES)
              && typeInfo
                   .getBaseType()
                   .equals(XmlSchemaBaseSimpleType.DECIMAL)) {

            /* If there is a restriction on the number of fraction
             * and/or total digits, we need to respect it.
             */
            int scale = 8;
            int precision = MathContext.DECIMAL128.getPrecision();

            HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>>
              facets = typeInfo.getFacets();

            if (facets != null) {

              // Fraction Digits are the scale
              final List<XmlSchemaRestriction> fractionDigitsFacets =
                  facets.get(XmlSchemaRestriction.Type.DIGITS_FRACTION);

              if ((fractionDigitsFacets != null)
                  && !fractionDigitsFacets.isEmpty()) {

                final XmlSchemaRestriction fractionDigitsFacet =
                    fractionDigitsFacets.get(0);

                final Object value = fractionDigitsFacet.getValue();
                if (value instanceof Number) {
                  scale = ((Number) value).intValue();
                } else {
                  try {
                    scale = Integer.parseInt(value.toString());
                  } catch (NumberFormatException nfe) {
                    throw new IllegalStateException(
                        "Fraction digits facet is not a number: " + value);
                  }
                }
              }

              // Total Digits are the precision
              final List<XmlSchemaRestriction> totalDigitsFacets =
                  facets.get(XmlSchemaRestriction.Type.DIGITS_TOTAL);
              if ((totalDigitsFacets != null)
                  && !totalDigitsFacets.isEmpty()) {

                final XmlSchemaRestriction totalDigitsFacet =
                    fractionDigitsFacets.get(0);

                final Object value = totalDigitsFacet.getValue();
                if (value instanceof Number) {
                  precision = ((Number) value).intValue();
                } else {
                  try {
                    precision = Integer.parseInt(value.toString());
                  } catch (NumberFormatException nfe) {
                    throw new IllegalStateException(
                        "Total digits facet is not a number: " + value);
                  }
                }
              }
            }

            schema.addProp("logicalType", new TextNode("decimal"));

            schema.addProp("scale", new IntNode(scale));

            schema.addProp("precision", new IntNode(precision));
          }
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

  static BigDecimal createBigDecimalFrom(byte[] bytes, Schema schema) {
    confirmIsValidDecimal(schema);
    return new BigDecimal(
        new BigInteger(bytes),
        getScaleFrom(schema),
        getMathContextFrom(schema));
  }

  static BigDecimal createBigDecimalFrom(String text, Schema schema) {
    confirmIsValidDecimal(schema);
    final int scale = getScaleFrom(schema);
    final MathContext mathContext = getMathContextFrom(schema);
    BigDecimal decimal = new BigDecimal(text, mathContext);
    if (decimal.scale() != scale) {
      decimal = decimal.setScale(scale, mathContext.getRoundingMode());
    }
    return decimal;
  }

  private static void confirmIsValidDecimal(Schema schema) {
    final JsonNode logicalTypeNode = schema.getJsonProp("logicalType");
    if (logicalTypeNode == null) {
      throw new IllegalStateException(
          "Attempted to read an XML Schema DECIMAL as an Avro "
          + "logical type, but the logical type is missing!");

    } else if (!"decimal".equals(logicalTypeNode.asText())) {
      throw new IllegalStateException(
          "Attempted to read an XML Schema DECIMAL as an Avro logical "
          + "type, but the logical type is " + logicalTypeNode);
    }
  }

  private static int getScaleFrom(Schema schema) {
    int scale = 0;

    final JsonNode scaleNode = schema.getJsonProp("scale");
    if (scaleNode != null) {
      if (!(scaleNode instanceof NumericNode)) {
        throw new IllegalStateException(
            "Attempted to read an XML Schema DECIMAL as an Avro logical "
                + "type, but the scale is not a number! Found: "
                + scaleNode);
      }

      scale = scaleNode.asInt();
    }

    return scale;
  }

  private static MathContext getMathContextFrom(Schema schema) {
    final JsonNode precisionNode = schema.getJsonProp("precision");

    if (precisionNode == null) {
      throw new IllegalArgumentException(
          "Attempted to read an XML Schema DECIMAL as an Avro "
          + "logical type, but the precision is missing!");

    } else if (!(precisionNode instanceof NumericNode)) {
      throw new IllegalArgumentException(
          "Attempted to read an XML Schema DECIMAL as an Avro logical "
              + "type, but the precision is not a number! Found: "
              + precisionNode);
    }

    return new MathContext( precisionNode.asInt() );
  }
}
