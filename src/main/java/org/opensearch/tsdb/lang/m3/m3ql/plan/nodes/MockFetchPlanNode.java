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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * MockFetchPlanNode represents a node in the M3QL plan that generates mock time series data based on provided values and tags.
 * It is primarily used for testing and demonstration purposes.
 */
public class MockFetchPlanNode extends M3PlanNode {
    private final List<Double> values;
    private final Map<String, String> tags;

    /**
     * Constructor for MockFetchPlanNode.
     *
     * @param id node id
     * @param values List of values for the mock series
     * @param tags Map of tag key-value pairs
     */
    public MockFetchPlanNode(int id, List<Double> values, Map<String, String> tags) {
        super(id);
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("MockFetch requires at least one value");
        }
        this.values = new ArrayList<>(values);
        this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "MOCK_FETCH(values=%s, tags=%s)", values, tags);
    }

    /**
     * Returns the list of values for the mock series.
     * @return list of values
     */
    public List<Double> getValues() {
        return new ArrayList<>(values);
    }

    /**
     * Returns the tags for the mock series.
     * @return map of tag key-value pairs
     */
    public Map<String, String> getTags() {
        return new HashMap<>(tags);
    }

    /**
     * Creates the mockFetch plan node from the corresponding AST Node.
     * @param functionNode The function AST node representing mockFetch
     * @return MockFetchPlanNode instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static MockFetchPlanNode of(FunctionNode functionNode) {
        if (functionNode == null) {
            throw new IllegalArgumentException("FunctionNode cannot be null");
        }

        List<M3ASTNode> children = functionNode.getChildren();
        if (children.isEmpty()) {
            throw new IllegalArgumentException("mockFetch requires at least one argument (value list)");
        }

        List<Double> values = new ArrayList<>();
        Map<String, String> tags = new HashMap<>();

        // Process all children: ValueNodes are values, TagKeyNodes are tags
        for (M3ASTNode child : children) {
            if (child instanceof ValueNode valueNode) {
                // Parse value and add to list (nan becomes NaN)
                try {
                    String value = valueNode.getValue();
                    if ("nan".equalsIgnoreCase(value)) {
                        values.add(Double.NaN);
                    } else {
                        values.add(Double.parseDouble(value));
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid numeric value: " + valueNode.getValue(), e);
                }
            } else if (child instanceof TagKeyNode tagKey) {
                // Process tag
                String keyName = tagKey.getKeyName();
                if (keyName == null || keyName.isEmpty()) {
                    throw new IllegalArgumentException("Key name cannot be empty in label specifiers");
                }
                String tagValue = getTagValueFromLabelKey(tagKey);
                tags.put(keyName, tagValue);
            } else {
                throw new IllegalArgumentException("Expected ValueNode or TagKeyNode, but found: " + child.getClass().getSimpleName());
            }
        }

        if (values.isEmpty()) {
            throw new IllegalArgumentException("mockFetch requires at least one value");
        }

        return new MockFetchPlanNode(M3PlannerContext.generateId(), values, tags);
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
