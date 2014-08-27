package org.apache.ws.commons.schema.walker;

import org.apache.ws.commons.schema.XmlSchemaAttribute;

public class XmlSchemaAttrInfo {

  XmlSchemaAttrInfo(XmlSchemaAttribute attribute, boolean isTopLevel) {
    this.attribute = attribute;
    this.isTopLevel = isTopLevel;
    this.attrType = null;
  }

  XmlSchemaAttrInfo(XmlSchemaAttribute attribute) {
    this(attribute, attribute.isTopLevel());
  }

  XmlSchemaAttrInfo(XmlSchemaAttribute attribute, XmlSchemaTypeInfo attrType) {
    this(attribute);
    this.attrType = attrType;
  }

  public XmlSchemaAttribute getAttribute() {
    return attribute;
  }

  public XmlSchemaTypeInfo getType() {
    return attrType;
  }

  public boolean isTopLevel() {
    return isTopLevel;
  }

  void setType(XmlSchemaTypeInfo attrType) {
    this.attrType = attrType;
  }

  private final XmlSchemaAttribute attribute;
  private final boolean isTopLevel;
  private XmlSchemaTypeInfo attrType;
}