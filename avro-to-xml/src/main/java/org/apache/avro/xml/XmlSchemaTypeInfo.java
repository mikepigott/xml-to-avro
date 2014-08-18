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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.namespace.QName;

/**
 * Represents an element's or attribute's type, meaning either a
 * {@link XmlSchemaBaseSimpleType} with facets, a union or list of
 * those, or a complex type.
 *
 * <p>
 * Also maintains a {@link QName} representing a type the user recognizes.
 * In the Avro case, this is the set of XML Schema simple types that can
 * be directly converted to Avro counterparts.
 * </p>
 */
final class XmlSchemaTypeInfo {

  private Type type;
  private HashMap<XmlSchemaRestriction.Type,List<XmlSchemaRestriction>> facets;
  private boolean isMixed;
  private XmlSchemaBaseSimpleType baseSimpleType;
  private QName userRecognizedType;
  private List<XmlSchemaTypeInfo> childTypes;

  enum Type {
    LIST,
    UNION,
    ATOMIC,
    COMPLEX;
  }

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

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder("XmlSchemaTypeInfo [");
    str.append(type).append("] Base Type: ").append(baseSimpleType);
    str.append(" User Recognized Type: ").append(userRecognizedType);
    str.append(" Is Mixed: ").append(isMixed);
    str.append(" Num Children: ");
    str.append((childTypes == null) ? 0 : childTypes.size());
    return str.toString();
  }
}
