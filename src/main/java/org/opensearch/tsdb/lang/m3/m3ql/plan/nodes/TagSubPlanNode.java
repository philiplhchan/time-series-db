/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * TagSubPlanNode represents a node in the M3QL plan that performs regex substitution on tag values.
 * Takes a seriesList and three strings: the first identifies the tag key to modify,
 * the second is a regex pattern to search, and the third is the replacement string.
 * The replacement pattern supports backreferences.
 */
public class TagSubPlanNode extends M3PlanNode {
    private final String tagName;
    private final String searchPattern;
    private final String replacement;

    /**
     * Constructor for TagSubPlanNode
     * @param id unique identifier for this node
     * @param tagName the tag key to modify
     * @param searchPattern the regex pattern to search
     * @param replacement the replacement string (supports backreferences)
     */
    public TagSubPlanNode(int id, String tagName, String searchPattern, String replacement) {
        super(id);
        this.tagName = tagName;
        this.searchPattern = searchPattern;
        this.replacement = replacement;
    }

    /**
     * Get the tag name to modify.
     * @return the tag name
     */
    public String getTagName() {
        return tagName;
    }

    /**
     * Get the search pattern.
     * @return the regex search pattern
     */
    public String getSearchPattern() {
        return searchPattern;
    }

    /**
     * Get the replacement string.
     * @return the replacement string
     */
    public String getReplacement() {
        return replacement;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "TAG_SUB(tag=%s,search=%s,replace=%s)", tagName, searchPattern, replacement);
    }

    /**
     * Factory method to create a TagSubPlanNode from a FunctionNode.
     * Expects the function node to have exactly 3 children that are ValueNodes
     * representing tag name, search pattern, and replacement.
     *
     * @param functionNode the function node to convert
     * @return an instance of TagSubPlanNode
     */
    public static TagSubPlanNode of(FunctionNode functionNode) {
        int argCount = functionNode.getChildren().size();
        if (argCount != 3) {
            throw new IllegalArgumentException("TagSub function requires exactly 3 arguments: tag name, search pattern, and replacement");
        }

        // Parse tag name (first argument)
        if (!(functionNode.getChildren().get(0) instanceof ValueNode tagNode)) {
            throw new IllegalArgumentException("First argument must be a value representing tag name");
        }
        String tagName = Utils.stripDoubleQuotes(tagNode.getValue());

        // Parse search pattern (second argument)
        if (!(functionNode.getChildren().get(1) instanceof ValueNode searchNode)) {
            throw new IllegalArgumentException("Second argument must be a value representing search pattern");
        }
        String searchPattern = Utils.stripDoubleQuotes(searchNode.getValue());

        // Parse replacement (third argument)
        if (!(functionNode.getChildren().get(2) instanceof ValueNode replaceNode)) {
            throw new IllegalArgumentException("Third argument must be a value representing replacement string");
        }
        String replacement = Utils.stripDoubleQuotes(replaceNode.getValue());

        return new TagSubPlanNode(M3PlannerContext.generateId(), tagName, searchPattern, replacement);
    }
}
