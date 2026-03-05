/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagKeyNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * MockFetchLinePlanNode represents a node in the M3QL plan that generates mock time series data
 * with a constant value (horizontal line).
 * It is primarily used for testing and demonstration purposes.
 */
public class MockFetchLinePlanNode extends M3PlanNode {
    private final double value;
    private final Map<String, String> tags;

    /**
     * Constructor for MockFetchLinePlanNode.
     *
     * @param id node id
     * @param value Constant value for the horizontal line
     * @param tags Map of tag key-value pairs
     */
    public MockFetchLinePlanNode(int id, double value, Map<String, String> tags) {
        super(id);
        this.value = value;
        this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "MOCK_FETCH_LINE(value=%s, tags=%s)", value, tags);
    }

    /**
     * Returns the constant value.
     * @return constant value
     */
    public double getValue() {
        return value;
    }

    /**
     * Returns the tags for the mock series.
     * @return map of tag key-value pairs
     */
    public Map<String, String> getTags() {
        return new HashMap<>(tags);
    }

    /**
     * Creates the mockFetchLine plan node from the corresponding AST Node.
     * Expects arguments: value, followed by optional tags.
     *
     * @param functionNode The function AST node representing mockFetchLine
     * @return MockFetchLinePlanNode instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static MockFetchLinePlanNode of(FunctionNode functionNode) {
        if (functionNode == null) {
            throw new IllegalArgumentException("FunctionNode cannot be null");
        }

        List<M3ASTNode> children = functionNode.getChildren();
        if (children.isEmpty()) {
            throw new IllegalArgumentException("mockFetchLine requires at least 1 argument: value");
        }

        // Parse the required value argument
        double value;

        try {
            M3ASTNode valueNode = children.get(0);
            if (!(valueNode instanceof ValueNode)) {
                throw new IllegalArgumentException("First argument (value) must be a numeric value");
            }
            value = Double.parseDouble(((ValueNode) valueNode).getValue());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in mockFetchLine arguments", e);
        }

        // Parse optional tags (remaining children)
        Map<String, String> tags = new HashMap<>();
        for (int i = 1; i < children.size(); i++) {
            M3ASTNode child = children.get(i);
            if (child instanceof TagKeyNode tagKey) {
                String keyName = tagKey.getKeyName();
                if (keyName == null || keyName.isEmpty()) {
                    throw new IllegalArgumentException("Key name cannot be empty in label specifiers");
                }
                String tagValue = getTagValueFromLabelKey(tagKey);
                tags.put(keyName, tagValue);
            } else {
                throw new IllegalArgumentException("Expected TagKeyNode for tags, but found: " + child.getClass().getSimpleName());
            }
        }

        return new MockFetchLinePlanNode(M3PlannerContext.generateId(), value, tags);
    }

    /**
     * Extract tag value from a TagKeyNode.
     *
     * @param tagKey TagKeyNode containing tag value
     * @return tag value as string
     */
    private static String getTagValueFromLabelKey(TagKeyNode tagKey) {
        if (tagKey.getChildren().isEmpty()) {
            throw new IllegalArgumentException("TagKeyNode must have at least one child");
        }
        if (tagKey.getChildren().size() > 1) {
            throw new IllegalArgumentException("TagKeyNode can only have one child for filter values");
        }
        M3ASTNode child = tagKey.getChildren().getFirst();
        if (child instanceof TagValueNode tagValueNode) {
            return tagValueNode.getValue();
        }
        throw new IllegalArgumentException("Invalid value for label, got " + child.getClass().getSimpleName());
    }
}
