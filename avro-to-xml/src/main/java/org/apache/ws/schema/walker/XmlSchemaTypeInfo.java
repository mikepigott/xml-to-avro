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

package org.apache.ws.schema.walker;

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
public final class XmlSchemaTypeInfo {

  private Type type;
  private HashMap<XmlSchemaRestriction.Type,List<XmlSchemaRestriction>> facets;
  private boolean isMixed;
  private XmlSchemaBaseSimpleType baseSimpleType;
  private QName userRecognizedType;
  private List<XmlSchemaTypeInfo> childTypes;

  public enum Type {
    LIST,
    UNION,
    ATOMIC,
    COMPLEX;
  }

  public XmlSchemaTypeInfo(XmlSchemaTypeInfo listType) {
    type = Type.LIST;
    childTypes = new ArrayList<XmlSchemaTypeInfo>(1);
    childTypes.add(listType);

    isMixed = false;
    facets = null;
    userRecognizedType = null;
  }

  public XmlSchemaTypeInfo(
      XmlSchemaTypeInfo listType,
      HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets) {
    this(listType);
    this.facets = facets;
  }

  public XmlSchemaTypeInfo(List<XmlSchemaTypeInfo> unionTypes) {
    type = Type.UNION;
    childTypes = unionTypes;

    isMixed = false;
    facets = null;
    userRecognizedType = null;
  }

  public XmlSchemaTypeInfo(
      List<XmlSchemaTypeInfo> unionTypes,
      HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets) {
    this(unionTypes);
    this.facets = facets;
  }

  public XmlSchemaTypeInfo(XmlSchemaBaseSimpleType baseSimpleType) {
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

  public XmlSchemaTypeInfo(
      XmlSchemaBaseSimpleType baseSimpleType,
      HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets) {

    this(baseSimpleType);
    this.facets = facets;
  }

  public XmlSchemaTypeInfo(boolean isMixed) {
    type = Type.COMPLEX;
    baseSimpleType = XmlSchemaBaseSimpleType.ANYTYPE;
    this.isMixed = isMixed;

    facets = null;
    childTypes = null;
    userRecognizedType = null;
  }

  public HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> getFacets() {
    return facets;
  }

  public XmlSchemaBaseSimpleType getBaseType() {
    return baseSimpleType;
  }

  public Type getType() {
    return type;
  }

  public List<XmlSchemaTypeInfo> getChildTypes() {
    return childTypes;
  }

  public QName getUserRecognizedType() {
    return userRecognizedType;
  }

  public boolean isMixed() {
    return isMixed;
  }

  public void setUserRecognizedType(QName userRecType) {
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
