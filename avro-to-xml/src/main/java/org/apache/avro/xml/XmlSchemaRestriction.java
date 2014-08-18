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

import org.apache.ws.commons.schema.XmlSchemaEnumerationFacet;
import org.apache.ws.commons.schema.XmlSchemaFacet;
import org.apache.ws.commons.schema.XmlSchemaFractionDigitsFacet;
import org.apache.ws.commons.schema.XmlSchemaLengthFacet;
import org.apache.ws.commons.schema.XmlSchemaMaxExclusiveFacet;
import org.apache.ws.commons.schema.XmlSchemaMaxInclusiveFacet;
import org.apache.ws.commons.schema.XmlSchemaMaxLengthFacet;
import org.apache.ws.commons.schema.XmlSchemaMinExclusiveFacet;
import org.apache.ws.commons.schema.XmlSchemaMinInclusiveFacet;
import org.apache.ws.commons.schema.XmlSchemaMinLengthFacet;
import org.apache.ws.commons.schema.XmlSchemaPatternFacet;
import org.apache.ws.commons.schema.XmlSchemaTotalDigitsFacet;
import org.apache.ws.commons.schema.XmlSchemaWhiteSpaceFacet;

/**
 * This represents an {@link XmlSchemaFacet}.  It uses an enum to more easily
 * work with different facets, and its {@link #equals(Object)} and
 * {@link #hashCode()} reflect that only enumerations and patterns can have
 * multiple facets.
 */
class XmlSchemaRestriction {

  private Type type;
  private Object value;
  private boolean isFixed;

  enum Type {
		ENUMERATION,
		EXCLUSIVE_MIN,
		EXCLUSIVE_MAX,
		INCLUSIVE_MIN,
		INCLUSIVE_MAX,
		PATTERN,
		WHITESPACE,
		LENGTH,
		LENGTH_MAX,
		LENGTH_MIN,
		DIGITS_FRACTION,
		DIGITS_TOTAL;
	}

	XmlSchemaRestriction(XmlSchemaFacet facet) {
    if (facet instanceof XmlSchemaEnumerationFacet) {
      type = Type.ENUMERATION;
    } else if (facet instanceof XmlSchemaMaxExclusiveFacet) {
      type = Type.EXCLUSIVE_MAX;
    } else if (facet instanceof XmlSchemaMaxInclusiveFacet) {
      type = Type.INCLUSIVE_MAX;
    } else if (facet instanceof XmlSchemaMinExclusiveFacet) {
      type = Type.EXCLUSIVE_MIN;
    } else if (facet instanceof XmlSchemaMinInclusiveFacet) {
      type = Type.INCLUSIVE_MIN;
    } else if (facet instanceof XmlSchemaFractionDigitsFacet) {
      type = Type.DIGITS_FRACTION;
    } else if (facet instanceof XmlSchemaTotalDigitsFacet) {
      type = Type.DIGITS_TOTAL;
    } else if (facet instanceof XmlSchemaPatternFacet) {
      type = Type.PATTERN;
    } else if (facet instanceof XmlSchemaWhiteSpaceFacet) {
      type = Type.WHITESPACE;
    } else if (facet instanceof XmlSchemaLengthFacet) {
      type = Type.LENGTH;
    } else if (facet instanceof XmlSchemaMinLengthFacet) {
      type = Type.LENGTH_MIN;
    } else if (facet instanceof XmlSchemaMaxLengthFacet) {
      type = Type.LENGTH_MAX;
    } else {
      throw new IllegalArgumentException(
          "Unrecognized facet " + facet.getClass().getName());
    }

    value = facet.getValue();
	  isFixed = facet.isFixed();
  }

	XmlSchemaRestriction(Type type) {
		this.type = type;
		this.value = null;
		this.isFixed = false;
	}

	XmlSchemaRestriction(Type type, Object value, boolean isFixed) {
		this.type = type;
		this.value = value;
		this.isFixed = isFixed;
	}

	Type getType() {
		return type;
	}

	Object getValue() {
		return value;
	}

	boolean isFixed() {
		return isFixed;
	}

	void setValue(Object value) {
		this.value = value;
	}

	void setFixed(boolean isFixed) {
		this.isFixed = isFixed;
	}

	/**
	 * Generates a hash code based on the contents.
	 *
	 * If the type is an enumeration, then the isFixed and value
	 * elements are used in calculating the hash code.  All of
	 * the other Restrictions are considered to be the same.
	 *
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((type == null) ? 0 : type.hashCode());

		if ((type != null)
		    && ((type == Type.ENUMERATION) || (type == Type.PATTERN))) {

			result = prime * result + (isFixed ? 1231 : 1237);
			result = prime * result + ((value == null) ? 0 : value.hashCode());
		}
		return result;
	}

	/**
	 * Determines equality.
	 *
	 * If the type is an enumeration, then the isFixed and value
	 * elements are used determining equality.  All of the other
	 * Restrictions are considered to be equal to each other.
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		XmlSchemaRestriction other = (XmlSchemaRestriction) obj;
		if (type != other.type)
			return false;

		if ((type != null)
		    && ((type == Type.ENUMERATION) || (type == Type.PATTERN))) {

		  if (isFixed != other.isFixed)
				return false;
			if (value == null) {
				if (other.value != null)
					return false;
			} else if (!value.equals(other.value))
				return false;
		}
		return true;
	}

	public String toString() {
		return type.name() + ": " + value + " (Fixed: " + isFixed + ")";
	}
}
