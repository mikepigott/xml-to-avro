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

package mpigott.sql.xml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a table in an SQL-based relational database.
 *
 * @author  Mike Pigott
 */
public final class SqlTable {

  private final String name;
  private final Map<SqlRelationship, List<SqlTable>> relationships;
  private final List<SqlAttribute> attributes;
  private final List<Integer> pathFromParent;

  public SqlTable(String name, List<Integer> pathFromParent) {
    this.name = name;
    this.pathFromParent = pathFromParent;
    this.relationships = new HashMap<SqlRelationship, List<SqlTable>>();
    this.attributes = new ArrayList<SqlAttribute>();
  }

  public String getName() {
    return name;
  }

  public void addRelationship(SqlRelationship relationship, SqlTable table) {
    List<SqlTable> tables = relationships.get(relationship);
    if (tables == null) {
      tables = new ArrayList<SqlTable>();
      relationships.put(relationship, tables);
    }
    tables.add(table);
  }

  public void addAttribute(SqlAttribute attribute) {
    attributes.add(attribute);
  }

  public List<Integer> pathFromParent() {
    return pathFromParent;
  }
}
