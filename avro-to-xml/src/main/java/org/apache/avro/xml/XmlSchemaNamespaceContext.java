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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ws.commons.schema.constants.Constants;
import org.apache.ws.commons.schema.utils.NamespacePrefixList;

/**
 * A {@link javax.xml.namespace.NamespaceContext}.
 *
 * <p>
 * Implemented as a series of scope-based stacks, one per prefix.
 * </p>
 */
final class XmlSchemaNamespaceContext implements NamespacePrefixList {

  private Map<String, List<String>> namespacesByPrefixStack;

  XmlSchemaNamespaceContext() {
    namespacesByPrefixStack = new HashMap<String, List<String>>();

    namespacesByPrefixStack.put(
        Constants.XML_NS_PREFIX,
        Collections.singletonList(Constants.XML_NS_URI));

    namespacesByPrefixStack.put(
        Constants.XMLNS_ATTRIBUTE,
        Collections.singletonList(Constants.XMLNS_ATTRIBUTE_NS_URI));
  }

  @Override
  public String getNamespaceURI(String prefix) {
    if (prefix == null) {
      throw new IllegalArgumentException("Prefix cannot be null.");
    }
    final List<String> namespaces = namespacesByPrefixStack.get(prefix);

    String namespace = null;
    if ((namespaces == null) || namespaces.isEmpty()) {
      namespace = Constants.NULL_NS_URI;
    } else {
      namespace = namespaces.get(namespaces.size() - 1);
    }

    return namespace;
  }

  @Override
  public String getPrefix(String namespaceUri) {
    if (namespaceUri == null) {
      throw new IllegalArgumentException("Namespace cannot be null.");
    }

    for (Map.Entry<String, List<String>> prefixEntry
        : namespacesByPrefixStack.entrySet()) {

      final List<String> namespaceStack = prefixEntry.getValue();
      if((namespaceStack != null)
          && !namespaceStack.isEmpty()
          && namespaceStack
               .get(namespaceStack.size() - 1)
               .equals(namespaceUri)) {
        return prefixEntry.getKey();
      }
    }

    return null;
  }

  @Override
  public Iterator getPrefixes(String namespaceUri) {
    if (namespaceUri == null) {
      throw new IllegalArgumentException("The Namespace URI cannot be null.");
    }

    ArrayList<String> prefixes = new ArrayList<String>();

    for (Map.Entry<String, List<String>> prefixEntry
          : namespacesByPrefixStack.entrySet()) {

      final List<String> namespaceStack = prefixEntry.getValue();
      if((namespaceStack != null)
          && !namespaceStack.isEmpty()
          && namespaceStack
               .get(namespaceStack.size() - 1)
               .equals(namespaceUri)) {

        prefixes.add( prefixEntry.getKey() );
      }
    }

    return prefixes.iterator();
  }

  @Override
  public String[] getDeclaredPrefixes() {
    final Set<String> prefixes = namespacesByPrefixStack.keySet();
    return prefixes.toArray(new String[prefixes.size()]);
  }

  /**
   * Adds a new prefix mapping to the context.  Returns true
   * if the mapping is new, and <code>false</code> if it already existed. 
   */
  void addNamespace(String prefix, String namespaceUri) {
    if ((prefix == null)
        || (namespaceUri == null)
        || namespaceUri.isEmpty()) {

      throw new IllegalArgumentException(
          "The prefix may not be null, and the namespace URI "
          + "may neither be null nor empty.");
    }

    List<String> namespaceStack = namespacesByPrefixStack.get(prefix);
    if (namespaceStack == null) {
      namespaceStack = new ArrayList<String>(1);
      namespacesByPrefixStack.put(prefix, namespaceStack);
    }
    namespaceStack.add(namespaceUri);
  }

  void removeNamespace(String prefix) {
    final List<String> namespaceStack = namespacesByPrefixStack.get(prefix);
    if ((namespaceStack == null) || namespaceStack.isEmpty()) {
      throw new IllegalStateException(
          "Prefix \""
          + prefix
          + "\" is not mapped to any namespaces.");
    }
    namespaceStack.remove(namespaceStack.size() - 1);
    if (namespaceStack.isEmpty()) {
      namespacesByPrefixStack.remove(prefix);
    }
  }

  void clear() {
    namespacesByPrefixStack.clear();
  }
}
