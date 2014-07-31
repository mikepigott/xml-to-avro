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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchemaContentType;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Represents the an Avro {@link Schema.Type} and its corresponding
 * {@link org.apache.ws.commons.schema.XmlSchemaType}, encoded in JSON.
 *
 * <p>
 * In cases where we do not have access to the original XML Schema,
 * the XML Schema Type encoded in JSON will disambiguate the lower
 * fidelity of the Avro schema type.
 * </p>
 *
 * @author  Mike Pigott
 */
final class XmlSchemaTypeInfo {

  enum Type {
    LIST,
    UNION,
    ATOMIC,
    COMPLEX;
  }

  Schema getAvroType() {
    return avroSchemaType;
  }

  JsonNode getXmlSchemaType() {
    return xmlSchemaType;
  }

  /**
   * Creates a JSON representation of the base type and
   * the restrictions imposed on it.  This will look like:
   *
   * <pre>
   * {
   *   "baseType": { <type info of base> },
   *   "facets": [
   *      {
   *        "type": "<type: LENGTH | LENGTH_MIN | LENGTH_MAX | ...>",
   *        "value" "<value>",
   *        "fixed": true/false
   *      },
   *      ...
   *   ]
   * }
   * </pre>
   */
  JsonNode getXmlSchemaAsJson() {
    ObjectNode type = JsonNodeFactory.instance.objectNode();
    type.put("baseType", xmlSchemaType);

    if ((facets != null) && !facets.isEmpty()) {
      ArrayNode facetsArray = JsonNodeFactory.instance.arrayNode();

      for (Map.Entry<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facetsForType : facets.entrySet()) {
        for (XmlSchemaRestriction facet : facetsForType.getValue()) {
          ObjectNode facetNode = JsonNodeFactory.instance.objectNode();
          facetNode.put("type", facet.getType().name());
          facetNode.put("value", facet.getValue().toString());
          facetNode.put("fixed", facet.isFixed());
          facetsArray.add(facetNode);
        }
      }
    
      type.put("facets", facetsArray);
    }

    return type;
  }

  // REWRITE STARTS HERE

  XmlSchemaTypeInfo(XmlSchemaTypeInfo listType) {
    type = Type.LIST;
    childTypes = new ArrayList<XmlSchemaTypeInfo>(1);
    childTypes.add(listType);

    isMixed = false;
    facets = null;
    userRecognizedType = null;
  }

  XmlSchemaTypeInfo(
      XmlSchemaTypeInfo listType,
      HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets) {
    this(listType);
    this.facets = facets;
  }

  XmlSchemaTypeInfo(List<XmlSchemaTypeInfo> unionTypes) {
    type = Type.UNION;
    childTypes = unionTypes;

    isMixed = false;
    facets = null;
    userRecognizedType = null;
  }

  XmlSchemaTypeInfo(
      List<XmlSchemaTypeInfo> unionTypes,
      HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets) {
    this(unionTypes);
    this.facets = facets;
  }

  XmlSchemaTypeInfo(XmlSchemaBaseSimpleType baseSimpleType) {
    if (baseSimpleType.equals(XmlSchemaBaseSimpleType.ANYTYPE)) {
      type = Type.COMPLEX;
    } else {
      type = Type.ATOMIC;
    }

    this.baseSimpleType = baseSimpleType;

    isMixed = false;
    facets = null;
    childTypes = null;
    userRecognizedType = null;
  }

  XmlSchemaTypeInfo(
      XmlSchemaBaseSimpleType baseSimpleType,
      HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets) {

    this(baseSimpleType);
    this.facets = facets;
  }

  XmlSchemaTypeInfo(boolean isMixed) {
    type = Type.COMPLEX;
    baseSimpleType = XmlSchemaBaseSimpleType.ANYTYPE;
    this.isMixed = isMixed;

    facets = null;
    childTypes = null;
    userRecognizedType = null;
  }

  HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> getFacets() {
    return facets;
  }

  XmlSchemaBaseSimpleType getBaseType() {
    return baseSimpleType;
  }

  Type getType() {
    return type;
  }

  List<XmlSchemaTypeInfo> getChildTypes() {
    return childTypes;
  }

  QName getUserRecognizedType() {
    return userRecognizedType;
  }

  boolean isMixed() {
    return isMixed;
  }

  void setUserRecognizedType(QName userRecType) {
    userRecognizedType = userRecType;
  }

  private Schema avroSchemaType;
  private JsonNode xmlSchemaType;

  private Type type;
  private HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets;
  private boolean isMixed;
  private XmlSchemaBaseSimpleType baseSimpleType;
  private QName userRecognizedType;
  private List<XmlSchemaTypeInfo> childTypes;
}
