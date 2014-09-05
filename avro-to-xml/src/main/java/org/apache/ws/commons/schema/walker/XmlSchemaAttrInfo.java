package org.apache.ws.commons.schema.walker;

import org.apache.ws.commons.schema.XmlSchemaAttribute;

/**
 * This represents a complete XML Schema Attribute,
 * after references are followed and types are defined.
 */
public class XmlSchemaAttrInfo {

  /**
   * Constructs a new <code>XmlSchemaAttrInfo</code> from the provided
   * {@link XmlSchemaAttribute} and {@link XmlSchemaTypeInfo}.
   *
   * <p>
   * The <code>XmlSchemaAttribute</code> represents the XML attribute
   * definition after any references have been resolved, and merged
   * with the global definition. 
   * </p>
   *
   * @param attribute The underlying <code>XmlSchemaAttribute</code>.
   * @param attrType  The attribute's type.
   */
  public XmlSchemaAttrInfo(
      XmlSchemaAttribute attribute,
      XmlSchemaTypeInfo attrType) {

    this(attribute);
    this.attrType = attrType;
  }

  XmlSchemaAttrInfo(XmlSchemaAttribute attribute, boolean isTopLevel) {
    this.attribute = attribute;
    this.isTopLevel = isTopLevel;
    this.attrType = null;
  }

  XmlSchemaAttrInfo(XmlSchemaAttribute attribute) {
    this(attribute, attribute.isTopLevel());
  }

  /**
   * The underlying {@link XmlSchemaAttribute}.  If the attribute was
   * originally a reference, this instance is merged with the global
   * attribute it referenced.
   *
   * <p>
   * The only exception is with {@link XmlSchemaAttribute#isTopLevel()}.
   * A copy of the <code>XmlSchemaAttribute</code> may have been made in
   * order to properly merge a local reference with a global definition.
   * When that happens, <code>XmlSchemaAttribute.isTopLevel()</code> may
   * not return the correct result.  Use {@link #isTopLevel()} instead.
   * </p>
   */
  public XmlSchemaAttribute getAttribute() {
    return attribute;
  }

  /**
   * The attribute's value type.
   */
  public XmlSchemaTypeInfo getType() {
    return attrType;
  }

  /**
   * Whether the attribute exists in the global namespace.  Because a
   * copy of {@link XmlSchemaAttribute} may have been made in order to
   * merge a local reference with the global definition,
   * {@link XmlSchemaAttribute#isTopLevel()} may no longer be accurate.
   */
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