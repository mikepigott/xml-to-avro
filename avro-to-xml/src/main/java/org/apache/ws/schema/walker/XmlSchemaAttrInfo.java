package org.apache.ws.schema.walker;

import org.apache.ws.commons.schema.XmlSchemaAttribute;

public class XmlSchemaAttrInfo {
  XmlSchemaAttrInfo(XmlSchemaAttribute attribute, XmlSchemaTypeInfo attrType) {
    this.attribute = attribute;
    this.attrType = attrType;
  }

  public XmlSchemaAttribute getAttribute() {
    return attribute;
  }

  public XmlSchemaTypeInfo getType() {
    return attrType;
  }

  private final XmlSchemaAttribute attribute;
  private final XmlSchemaTypeInfo attrType;
}