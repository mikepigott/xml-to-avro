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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaAttributeGroup;
import org.apache.ws.commons.schema.XmlSchemaAttributeGroupMember;
import org.apache.ws.commons.schema.XmlSchemaAttributeGroupRef;
import org.apache.ws.commons.schema.XmlSchemaAttributeOrGroupRef;
import org.apache.ws.commons.schema.XmlSchemaComplexContentExtension;
import org.apache.ws.commons.schema.XmlSchemaComplexContentRestriction;
import org.apache.ws.commons.schema.XmlSchemaComplexType;
import org.apache.ws.commons.schema.XmlSchemaContent;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaFacet;
import org.apache.ws.commons.schema.XmlSchemaFractionDigitsFacet;
import org.apache.ws.commons.schema.XmlSchemaMaxInclusiveFacet;
import org.apache.ws.commons.schema.XmlSchemaMinInclusiveFacet;
import org.apache.ws.commons.schema.XmlSchemaParticle;
import org.apache.ws.commons.schema.XmlSchemaPatternFacet;
import org.apache.ws.commons.schema.XmlSchemaSequence;
import org.apache.ws.commons.schema.XmlSchemaSequenceMember;
import org.apache.ws.commons.schema.XmlSchemaSimpleContentExtension;
import org.apache.ws.commons.schema.XmlSchemaSimpleContentRestriction;
import org.apache.ws.commons.schema.XmlSchemaSimpleType;
import org.apache.ws.commons.schema.XmlSchemaSimpleTypeContent;
import org.apache.ws.commons.schema.XmlSchemaSimpleTypeList;
import org.apache.ws.commons.schema.XmlSchemaSimpleTypeRestriction;
import org.apache.ws.commons.schema.XmlSchemaSimpleTypeUnion;
import org.apache.ws.commons.schema.XmlSchemaType;
import org.apache.ws.commons.schema.XmlSchemaUse;
import org.apache.ws.commons.schema.XmlSchemaWhiteSpaceFacet;
import org.apache.ws.commons.schema.constants.Constants;
import org.apache.ws.commons.schema.utils.XmlSchemaNamed;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

/**
 * The scope represents the set of types, attributes, and
 * child groups & elements that the current type represents.
 *
 * @author  Mike Pigott
 */
final class XmlSchemaScope {

  private static final Map<QName, Schema.Type> xmlToAvroTypeMap =
      new HashMap<QName, Schema.Type>();

  private static final Map<QName, List<XmlSchemaFacet>> facetsOfSchemaTypes =
      new HashMap<QName, List<XmlSchemaFacet>>();

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

    /* Until https://issues.apache.org/jira/browse/XMLSCHEMA-33
     * makes it to the next release of Apache XML Schema (2.1.1).
     */
    facetsOfSchemaTypes.put(
        Constants.XSD_DURATION,
        Arrays.asList( new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_DATETIME,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_TIME,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_DATE,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_YEARMONTH,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_YEAR,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_MONTHDAY,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_DAY,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_MONTH,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_MONTH,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_BOOLEAN,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_BASE64,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_HEXBIN,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_FLOAT,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_DOUBLE,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_ANYURI,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_QNAME,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_DECIMAL,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", true) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_INTEGER,
        Arrays.asList(new XmlSchemaFacet[] {
            new XmlSchemaFractionDigitsFacet(new Integer(0), true),
            new XmlSchemaPatternFacet("[\\-+]?[0-9]+", false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_NONPOSITIVEINTEGER,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaMaxInclusiveFacet(new Integer(0), false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_NEGATIVEINTEGER,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaMaxInclusiveFacet(new Integer(-1), false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_LONG,
        Arrays.asList(new XmlSchemaFacet[] {
            new XmlSchemaMinInclusiveFacet(new Long(-9223372036854775808L), false),
            new XmlSchemaMaxInclusiveFacet(new Long(9223372036854775807L), false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_INT,
        Arrays.asList(new XmlSchemaFacet[] {
            new XmlSchemaMinInclusiveFacet(new Integer(-2147483648), false),
            new XmlSchemaMaxInclusiveFacet(2147483647, false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_SHORT,
        Arrays.asList(new XmlSchemaFacet[] {
            new XmlSchemaMinInclusiveFacet(new Short((short) -32768), false),
            new XmlSchemaMaxInclusiveFacet(new Short((short) 32767), false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_BYTE,
        Arrays.asList(new XmlSchemaFacet[] {
            new XmlSchemaMinInclusiveFacet(new Byte((byte) -128), false),
            new XmlSchemaMaxInclusiveFacet(new Byte((byte) 127), false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_NONNEGATIVEINTEGER,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaMinInclusiveFacet(new Integer(0), false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_POSITIVEINTEGER,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaMinInclusiveFacet(new Integer(1), false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_UNSIGNEDLONG,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaMaxInclusiveFacet(new BigInteger("18446744073709551615"), false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_UNSIGNEDINT,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaMaxInclusiveFacet(new Long(4294967295L), false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_UNSIGNEDSHORT,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaMaxInclusiveFacet(new Integer(65535), false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_UNSIGNEDBYTE,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaMaxInclusiveFacet(new Short((short) 255), false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_STRING,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("preserve", false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_NORMALIZEDSTRING,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("replace", false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_TOKEN,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaWhiteSpaceFacet("collapse", false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_LANGUAGE,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaPatternFacet("[a-zA-Z]{1,8}(-[a-zA-Z0-9]{1,8})*", false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_NMTOKEN,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaPatternFacet("\\c+", false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_NAME,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaPatternFacet("\\i\\c*", false) }));

    facetsOfSchemaTypes.put(
        Constants.XSD_NCNAME,
        Arrays.asList(new XmlSchemaFacet[] { new XmlSchemaPatternFacet("[\\i-[:]][\\c-[:]]*", false) }));
  }

  /**
   * Initialization of members to be filled in during the walk.
   */
  private XmlSchemaScope() {
    typeInfo = null;
    attributes = null;
    child = null;
  }

  private XmlSchemaScope(XmlSchemaScope child, XmlSchemaType type) {
    this();
    this.substitutes = child.substitutes;
    this.schemasByNamespace = child.schemasByNamespace;

    walk(type);
  }

  /**
   * Initializes a new {@link XmlSchemaScope} with a base
   * {@link XmlSchemaElement}.  The element type and
   * attributes will be traversed, and attribute lists
   * and element children will be retrieved.
   *
   * @param element The base element to build the scope from.
   * @param substitutions The master list of substitution groups to pull from.
   */
  XmlSchemaScope(
      XmlSchemaType type,
      Map<String, XmlSchema> xmlSchemasByNamespace,
      Map<QName, List<XmlSchemaElement>> substitutions) {

    this();

    schemasByNamespace = xmlSchemasByNamespace; 
    substitutes = substitutions;

    walk(type);
  }

  XmlSchemaTypeInfo getTypeInfo() {
    return typeInfo;
  }

  Collection<XmlSchemaAttribute> getAttributesInScope() {
    if (attributes == null) {
      return null;
    }
    return attributes.values();
  }

  XmlSchemaParticle getParticle() {
    return child;
  }

  private void walk(XmlSchemaType type) {
    if (type instanceof XmlSchemaSimpleType) {
      walk((XmlSchemaSimpleType) type);
    } else if (type instanceof XmlSchemaComplexType) {
      walk((XmlSchemaComplexType) type);
    } else {
      throw new IllegalArgumentException("Unrecognized XmlSchemaType of type " + type.getClass().getName());
    }
  }

  private void walk(XmlSchemaSimpleType simpleType) {
    XmlSchemaSimpleTypeContent content = simpleType.getContent();

    if (content == null) {
      /* Only anyType contains no content. We
       * reached the root of the type hierarchy.
       */
      typeInfo =
          new XmlSchemaTypeInfo(
              Schema.create(xmlToAvroTypeMap.get(simpleType.getQName())),
              createJsonNodeFor(simpleType.getQName()));

    } else if ( xmlToAvroTypeMap.containsKey(simpleType.getQName()) ) {
      // This is a recognized Avro type.  Use it!
      typeInfo =
          new XmlSchemaTypeInfo(
              Schema.create(xmlToAvroTypeMap.get(simpleType.getQName())),
              createJsonNodeFor(simpleType.getQName()));

    } else if (content instanceof XmlSchemaSimpleTypeList) {
        XmlSchemaSimpleTypeList list = (XmlSchemaSimpleTypeList) content;
        XmlSchemaSimpleType listType = list.getItemType();
        if (listType == null) {
            XmlSchema schema = schemasByNamespace.get( list.getItemTypeName().getNamespaceURI() );
            listType = (XmlSchemaSimpleType) schema.getTypeByName(list.getItemTypeName());
        }
        if (listType == null) {
            throw new IllegalArgumentException("Unrecognized schema type for list " + getName(simpleType, "{Anonymous List Type}"));
        }

        XmlSchemaScope parentScope = new XmlSchemaScope(this, listType);
        typeInfo =
            new XmlSchemaTypeInfo(
                Schema.createArray( parentScope.getTypeInfo().getAvroType() ),
                createJsonNodeForList( parentScope.getTypeInfo().getXmlSchemaType() ));

    } else if (content instanceof XmlSchemaSimpleTypeUnion) {
        XmlSchemaSimpleTypeUnion union = (XmlSchemaSimpleTypeUnion) content;
        QName[] namedBaseTypes = union.getMemberTypesQNames();

        List<XmlSchemaSimpleType> baseTypes = union.getBaseTypes();

        if (namedBaseTypes != null) {
          if (baseTypes == null) {
            baseTypes = new ArrayList<XmlSchemaSimpleType>(namedBaseTypes.length);
          }

          for (QName namedBaseType : namedBaseTypes) {
            XmlSchema schema = schemasByNamespace.get( namedBaseType.getNamespaceURI() );
            XmlSchemaSimpleType baseType = (XmlSchemaSimpleType) schema.getTypeByName(namedBaseType);
            if (baseType != null) {
                baseTypes.add(baseType);
            }
          }
        }

        // baseTypes cannot be null at this point; there must be a union of types.
        if ((baseTypes == null) || baseTypes.isEmpty()) {
          throw new IllegalArgumentException("Unrecognized base types for union " + getName(simpleType, "{Anonymous Union Type}"));
        }

        ArrayList<Schema> unionSchemas = new ArrayList<Schema>( baseTypes.size() );
        ArrayList<JsonNode> unionNodes = new ArrayList<JsonNode>( baseTypes.size() );
        for (XmlSchemaSimpleType baseType : baseTypes) {
          XmlSchemaScope parentScope = new XmlSchemaScope(this, baseType);
          unionSchemas.add( parentScope.getTypeInfo().getAvroType() );
          unionNodes.add( parentScope.getTypeInfo().getXmlSchemaType() );
        }

        typeInfo =
            new XmlSchemaTypeInfo(
                Schema.createUnion(unionSchemas),
                createJsonNodeForUnion(unionNodes));

    } else if (content instanceof XmlSchemaSimpleTypeRestriction) {
        XmlSchemaSimpleTypeRestriction restr = (XmlSchemaSimpleTypeRestriction) content;

        XmlSchemaSimpleType baseType = restr.getBaseType();
        if (baseType == null) {
            XmlSchema schema = schemasByNamespace.get( restr.getBaseTypeName().getNamespaceURI() );
            baseType = (XmlSchemaSimpleType) schema.getTypeByName(restr.getBaseTypeName());
        }

        List<XmlSchemaFacet> facets = restr.getFacets();
        if ((facets == null) || facets.isEmpty()) {
          // Needed until XMLSCHEMA-33 becomes available.
          facets = facetsOfSchemaTypes.get( simpleType.getQName() );
        }

        if (baseType != null) {
          XmlSchemaScope parentScope = new XmlSchemaScope(this, baseType);

          /* We need to track the original type as well as the set of facets
           * imposed on that type.  Once the recursion ends, and we make it
           * all the way back to the first scope, we can create a JSON node
           * that represents the derived type and all of its imposed facets.
           */
          typeInfo =
              new XmlSchemaTypeInfo(
                  parentScope.getTypeInfo().getAvroType(),
                  parentScope.getTypeInfo().getXmlSchemaType(),
                  mergeFacets(parentScope.getTypeInfo().getFacets(), facets));
        } else {
            throw new IllegalArgumentException("Unrecognized base type for " + getName(simpleType, "{Anonymous Simple Type}"));
        }
    } else {
        throw new IllegalArgumentException("XmlSchemaSimpleType " + getName(simpleType, "{Anonymous Simple Type}") + "contains unrecognized XmlSchemaSimpleTypeContent " + content.getClass().getName());
    }
  }

  private void walk(XmlSchemaComplexType complexType) {
    XmlSchemaContent complexContent =
        (complexType.getContentModel() != null)
        ? complexType.getContentModel().getContent()
        : null;

    /* Process the complex type extensions and restrictions.
     * If there aren't any, the content is be defined by the particle.
     */
    if (complexContent != null) {
      walk(complexContent);

    } else {
      child = complexType.getParticle();
      attributes = createAttributeMap( complexType.getAttributes() );
    }
  }

  private void walk(XmlSchemaContent content) {

    if (content instanceof XmlSchemaComplexContentExtension) {
      XmlSchemaComplexContentExtension ext = (XmlSchemaComplexContentExtension) content;

      XmlSchema schema = schemasByNamespace.get( ext.getBaseTypeName().getNamespaceURI() );
      XmlSchemaType baseType = schema.getTypeByName( ext.getBaseTypeName() );

      XmlSchemaParticle baseParticle = null;

      if (baseType != null) {
        /* Complex content extensions add attributes and elements
         * in addition to what was retrieved from the parent. Since
         * there will be no collisions, it is safe to perform a
         * straight add.
         */
        XmlSchemaScope parentScope = new XmlSchemaScope(this, baseType);
        Collection<XmlSchemaAttribute> parentAttrs = parentScope.getAttributesInScope();

        attributes = createAttributeMap( ext.getAttributes() );
        for (XmlSchemaAttribute parentAttr : parentAttrs) {
          attributes.put(parentAttr.getQName(), parentAttr);
        }

        baseParticle = parentScope.getParticle();
      }

      /* An extension of a complex type is equivalent to creating a sequence of
       * two particles: the parent particle followed by the child particle.
       */
      if (ext.getParticle() == null) {
        child = baseParticle;
      } else if (baseParticle == null) {
        child = ext.getParticle();
      } else {
        XmlSchemaSequence seq = new XmlSchemaSequence();
        seq.getItems().add((XmlSchemaSequenceMember) baseParticle);
        seq.getItems().add((XmlSchemaSequenceMember) ext.getParticle());
        child = seq;
      }

    } else if (content instanceof XmlSchemaComplexContentRestriction) {
      XmlSchemaComplexContentRestriction rstr = (XmlSchemaComplexContentRestriction) content;
      XmlSchema schema = schemasByNamespace.get( rstr.getBaseTypeName().getNamespaceURI() );
      XmlSchemaType baseType = schema.getTypeByName( rstr.getBaseTypeName() );
      Map<QName, XmlSchemaAttribute> parentAttrs = null;

      if (baseType != null) {
        XmlSchemaScope parentScope = new XmlSchemaScope(this, baseType);

        attributes =
            mergeAttributes(
                parentScope.attributes,
                createAttributeMap( rstr.getAttributes() ));

        child = parentScope.getParticle();
      }

      /* There is no inheritance when restricting particles.  If the schema
       * writer wishes to include elements in the parent type, (s)he must
       * redefine them in the child.
       */
      if (rstr.getParticle() != null) {
        child = rstr.getParticle();
      }

    } else if (content instanceof XmlSchemaSimpleContentExtension) {
      XmlSchemaSimpleContentExtension ext = (XmlSchemaSimpleContentExtension) content;
      attributes = createAttributeMap( ext.getAttributes() );

      XmlSchema schema = schemasByNamespace.get( ext.getBaseTypeName().getNamespaceURI() );
      XmlSchemaSimpleType baseType = (XmlSchemaSimpleType) schema.getTypeByName( ext.getBaseTypeName() );

      if (baseType != null) {
        XmlSchemaScope parentScope = new XmlSchemaScope(this, baseType);

        typeInfo =
            new XmlSchemaTypeInfo(
                parentScope.getTypeInfo().getAvroType(),
                parentScope.getTypeInfo().getXmlSchemaType(),
                parentScope.getTypeInfo().getFacets());
      }

    } else if (content instanceof XmlSchemaSimpleContentRestriction) {
      XmlSchemaSimpleContentRestriction rstr = (XmlSchemaSimpleContentRestriction) content;
      attributes = createAttributeMap( rstr.getAttributes() );

      XmlSchemaSimpleType baseType = null;
      if (rstr.getBaseType() != null) {
        baseType = rstr.getBaseType();
      } else {
        XmlSchema schema = schemasByNamespace.get( rstr.getBaseTypeName().getNamespaceURI() );
        baseType = (XmlSchemaSimpleType) schema.getTypeByName( rstr.getBaseTypeName() );
      }

      if (baseType != null) {
        XmlSchemaScope parentScope = new XmlSchemaScope(this, baseType);
        typeInfo =
            new XmlSchemaTypeInfo(
                parentScope.getTypeInfo().getAvroType(),
                parentScope.getTypeInfo().getXmlSchemaType(),
                mergeFacets(parentScope.getTypeInfo().getFacets(), rstr.getFacets()));
      }
    }
  }

  private ArrayList<XmlSchemaAttribute> getAttributesOf(XmlSchemaAttributeGroupRef groupRef) {
    XmlSchemaAttributeGroup attrGroup = groupRef.getRef().getTarget();
    if (attrGroup == null) {
      XmlSchema schema = schemasByNamespace.get(groupRef.getTargetQName().getNamespaceURI());
      attrGroup = schema.getAttributeGroupByName(groupRef.getTargetQName());
    }
    return getAttributesOf(attrGroup);
  }

  private ArrayList<XmlSchemaAttribute> getAttributesOf(XmlSchemaAttributeGroup attrGroup) {
    ArrayList<XmlSchemaAttribute> attrs = new ArrayList<XmlSchemaAttribute>( attrGroup.getAttributes().size() );

    for (XmlSchemaAttributeGroupMember member : attrGroup.getAttributes()) {
      if (member instanceof XmlSchemaAttribute) {
        attrs.add( getAttribute((XmlSchemaAttribute) member) );

      } else if (member instanceof XmlSchemaAttributeGroup) {
        attrs.addAll( getAttributesOf((XmlSchemaAttributeGroup) member) );

      } else if (member instanceof XmlSchemaAttributeGroupRef) {
        attrs.addAll( getAttributesOf((XmlSchemaAttributeGroupRef) member) );

      } else {
        throw new IllegalArgumentException("Attribute Group " + getName(attrGroup, "{Anonymous Attribute Group}") + " contains unrecognized attribute group memeber type " + member.getClass().getName());
      }
    }

    return attrs;
  }

  private XmlSchemaAttribute getAttribute(XmlSchemaAttribute attribute) {
    if (!attribute.isRef()) {
      return attribute;
    }

    final QName attrQName = attribute.getRefBase().getTargetQName();
    final XmlSchema schema = schemasByNamespace.get( attrQName.getNamespaceURI() );

    XmlSchemaAttribute globalAttr = null;
    if (attribute.getRef().getTarget() != null) {
      globalAttr = attribute.getRef().getTarget();
    } else {
      globalAttr = schema.getAttributeByName(attrQName);
    }

    /* The attribute reference defines the attribute use and overrides the ID,
     * default, and fixed fields.  Everything else is defined by the global
     * attribute.
     */
    String fixedValue = attribute.getFixedValue();
    if (fixedValue != null) {
      fixedValue = globalAttr.getFixedValue();
    }

    String defaultValue = attribute.getDefaultValue();
    if ((defaultValue == null) && (fixedValue == null)) {
      defaultValue = globalAttr.getDefaultValue();
    }

    String id = attribute.getId();
    if (id == null) {
      id = globalAttr.getId();
    }

    final XmlSchemaAttribute copy = new XmlSchemaAttribute(schema, false);
    copy.setAnnotation( globalAttr.getAnnotation() );
    copy.setDefaultValue(defaultValue);
    copy.setFixedValue(fixedValue);
    copy.setForm( globalAttr.getForm() );
    copy.setId(id);
    copy.setLineNumber( attribute.getLineNumber() );
    copy.setLinePosition( attribute.getLinePosition() );
    copy.setMetaInfoMap( globalAttr.getMetaInfoMap() );
    copy.setName( globalAttr.getName() );
    copy.setSchemaType( globalAttr.getSchemaType() );
    copy.setSchemaTypeName( globalAttr.getSchemaTypeName() );
    copy.setSourceURI( globalAttr.getSourceURI() );
    copy.setUnhandledAttributes( globalAttr.getUnhandledAttributes() );
    copy.setUse( attribute.getUse() );

    return copy;
  }

  private Map<QName, XmlSchemaAttribute> createAttributeMap(List<XmlSchemaAttributeOrGroupRef> attrs) {
    if ((attrs == null) || attrs.isEmpty()) {
      return null;
    }

    Map<QName, XmlSchemaAttribute >attributes = new HashMap<QName, XmlSchemaAttribute>();
    for (XmlSchemaAttributeOrGroupRef attr : attrs) {
      if (attr instanceof XmlSchemaAttribute) {
        XmlSchemaAttribute attribute = getAttribute((XmlSchemaAttribute) attr);
        attributes.put(attribute.getQName(), attribute);
      } else if (attr instanceof XmlSchemaAttributeGroupRef) {
        final List<XmlSchemaAttribute> attrList =
            getAttributesOf((XmlSchemaAttributeGroupRef) attr);
        for (XmlSchemaAttribute attribute : attrList) {
          attributes.put(attribute.getQName(), attribute);
        }
      }
    }

    return attributes;
  }

  private static String getName(XmlSchemaNamed name, String defaultName) {
    if (name.isAnonymous()) {
      return defaultName;
    } else {
      return name.getName();
    }
  }

  private static JsonNode createJsonNodeFor(QName baseType) {
    ObjectNode object = JsonNodeFactory.instance.objectNode();
    object.put("namespace", baseType.getNamespaceURI());
    object.put("localPart", baseType.getLocalPart());
    return object;
  }

  private static JsonNode createJsonNodeForList(JsonNode child) {
    ObjectNode object = JsonNodeFactory.instance.objectNode();
    object.put("type", "list");
    object.put("value", child);
    return object;
  }

  private static JsonNode createJsonNodeForUnion(List<JsonNode> unionTypes) {
    ObjectNode object = JsonNodeFactory.instance.objectNode();
    object.put("type", "union");
    ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
    arrayNode.addAll(unionTypes);
    object.put("value", arrayNode);
    return object;
  }

  private static Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> mergeFacets(Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> parentFacets, List<XmlSchemaFacet> child) {
    if ((child == null) || child.isEmpty()) {
      return parentFacets;
    }

    HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> childFacets =
        new  HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>>( child.size() );

    for (XmlSchemaFacet facet : child) {
      XmlSchemaRestriction rstr = new XmlSchemaRestriction(facet);
      List<XmlSchemaRestriction> rstrList = childFacets.get( rstr.getType() );
      if (rstrList == null) {
        // Only enumerations may have more than one value.
        if (rstr.getType() == XmlSchemaRestriction.Type.ENUMERATION) {
          rstrList = new ArrayList<XmlSchemaRestriction>(5);
        } else {
          rstrList = new ArrayList<XmlSchemaRestriction>(1);
        }
        childFacets.put(rstr.getType(), rstrList);
      }
      rstrList.add(rstr);
    }

    if (parentFacets == null) {
      return childFacets;
    }

    // Child facets override parent facets
    for (Map.Entry<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> rstrEntry : childFacets.entrySet()) {
      parentFacets.put(rstrEntry.getKey(), rstrEntry.getValue());
    }

    return parentFacets;
  }

  private static Map<QName, XmlSchemaAttribute> mergeAttributes(Map<QName, XmlSchemaAttribute> parentAttrs, Map<QName, XmlSchemaAttribute> childAttrs) {
    if (parentAttrs == null) {
      return childAttrs;
    } else if (childAttrs == null) {
      return parentAttrs;
    }

    /* Child attributes inherit all parent attributes, but may
     * change the type, usage, default value, or fixed value.
     */
    for (Map.Entry<QName, XmlSchemaAttribute> parentAttrEntry : parentAttrs.entrySet()) {
      XmlSchemaAttribute parentAttr = parentAttrEntry.getValue();
      XmlSchemaAttribute childAttr = childAttrs.get( parentAttrEntry.getKey() );
      if (childAttr != null) {
        if (childAttr.getSchemaType() != null) {
          parentAttr.setSchemaType( childAttr.getSchemaType() );
        }
        if (childAttr.getUse() != XmlSchemaUse.NONE) {
          parentAttr.setUse( childAttr.getUse() );
        }

        // Attribute values may be defaulted or fixed, but not both.
        if (childAttr.getDefaultValue() != null) {
          parentAttr.setDefaultValue( childAttr.getDefaultValue() );
          parentAttr.setFixedValue(null);
        } else if (childAttr.getFixedValue() != null) {
          parentAttr.setFixedValue( childAttr.getFixedValue() );
          parentAttr.setDefaultValue(null);
        }
      }
    }

    return parentAttrs;
  }

  private Map<QName, List<XmlSchemaElement>> substitutes;
  private Map<String, XmlSchema> schemasByNamespace;

  private XmlSchemaTypeInfo typeInfo;
  private Map<QName, XmlSchemaAttribute> attributes;
  private XmlSchemaParticle child;
}
