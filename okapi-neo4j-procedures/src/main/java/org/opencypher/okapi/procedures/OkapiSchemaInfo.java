/**
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.okapi.procedures;

import java.util.List;

public class OkapiSchemaInfo
{
    /**
     * Indicates whether the entry is a node or a relationship
     */
    public final String type;
    /**
     * A combination of labels or a relationship
     */
    public final List<String> nodeLabelsOrRelType;
    /**
     * A property that occurs on the given label combination / relationship type
     */
    public final String property;
    /**
     * The CypherType of the given property on the given label combination / relationship type
     */
    public final String cypherType;
    /**
     * List of warnings created during schema creation.
     */
    public final List<String> warnings;

    public OkapiSchemaInfo(
            String type,
            List<String> nodeLabelsOrRelType,
            String property,
            String cypherType,
            List<String> warnings )
    {
        this.type = type;
        this.nodeLabelsOrRelType = nodeLabelsOrRelType;
        this.property = property;
        this.cypherType = cypherType;
        this.warnings = warnings;
    }
}
