/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.Constants;
import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Represents a percentileOfSeries plan node in the M3QL query plan.
 * <p>
 * Grammar: percentileOfSeries percentiles [true/false] [tags]
 * - percentiles: required list of floats
 * - true/false: optional interpolate flag (default: false, only in planning)
 * - tags: optional tags for group aggregation
 * <p>
 * Aliases: median and medianOfSeries
 * - Call percentileOfSeries with percentile=50, interpolate=false
 * - Optional tags argument
 */
public class PercentileOfSeriesPlanNode extends M3PlanNode {
    private final List<Float> percentiles;
    private final boolean interpolate;
    private final List<String> tags;

    /**
     * Constructs a PercentileOfSeriesPlanNode with the specified parameters.
     *
     * @param id node id
     * @param percentiles the list of percentiles to compute (e.g., [50.0, 95.0, 99.0])
     * @param interpolate whether to use interpolation when calculating percentiles
     * @param tags the list of tags for group aggregation (null means no grouping)
     */
    public PercentileOfSeriesPlanNode(int id, List<Float> percentiles, boolean interpolate, List<String> tags) {
        super(id);
        this.percentiles = percentiles;
        this.interpolate = interpolate;
        this.tags = tags;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(
            Locale.getDefault(),
            "PERCENTILE_OF_SERIES(percentiles=%s, interpolate=%s, groupBy=%s)",
            percentiles,
            interpolate,
            tags
        );
    }

    /**
     * Returns the list of percentiles to compute.
     * @return List of Float percentiles
     */
    public List<Float> getPercentiles() {
        return percentiles;
    }

    /**
     * Returns whether interpolation is enabled.
     * @return boolean indicating if interpolation is enabled
     */
    public boolean isInterpolate() {
        return interpolate;
    }

    /**
     * Returns the list of tags for group aggregation.
     * @return List of String tags, null indicates aggregate all series without grouping
     */
    public List<String> getTags() {
        return tags;
    }

    /**
     * Creates a PercentileOfSeriesPlanNode from the corresponding AST FunctionNode.
     * <p>
     * For percentileOfSeries: percentileOfSeries percentiles [true/false] [tags...]
     * For median/medianOfSeries: median|medianOfSeries [tags...]
     *
     * @param functionNode the function AST node
     * @return the constructed PercentileOfSeriesPlanNode
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static PercentileOfSeriesPlanNode of(FunctionNode functionNode) {
        String functionName = functionNode.getFunctionName();

        // Handle median and medianOfSeries aliases
        if (Constants.Functions.MEDIAN.equals(functionName) || Constants.Functions.MEDIAN_OF_SERIES.equals(functionName)) {
            return createMedianPlanNode(functionNode);
        }

        // Handle percentileOfSeries
        if (Constants.Functions.PERCENTILE_OF_SERIES.equals(functionName)) {
            return createPercentileOfSeriesPlanNode(functionNode);
        }

        throw new IllegalArgumentException("Unknown function: " + functionName);
    }

    /**
     * Creates a plan node for median/medianOfSeries functions.
     * Grammar: median|medianOfSeries [tags...]
     */
    private static PercentileOfSeriesPlanNode createMedianPlanNode(FunctionNode functionNode) {
        List<String> tags = null;

        // Parse optional tags
        if (functionNode.getChildren() != null && !functionNode.getChildren().isEmpty()) {
            tags = new ArrayList<>();
            for (M3ASTNode astNode : functionNode.getChildren()) {
                if (astNode instanceof ValueNode node) {
                    tags.add(Utils.stripDoubleQuotes(node.getValue()));
                }
            }
        }

        // Median is 50th percentile with no interpolation
        List<Float> percentiles = new ArrayList<>();
        percentiles.add(50.0f);

        return new PercentileOfSeriesPlanNode(M3PlannerContext.generateId(), percentiles, false, tags);
    }

    /**
     * Creates a plan node for percentileOfSeries function.
     * Grammar: percentileOfSeries percentiles [true/false] [tags...]
     * Note: M3QL parser splits comma-separated values into separate VALUE nodes
     */
    private static PercentileOfSeriesPlanNode createPercentileOfSeriesPlanNode(FunctionNode functionNode) {
        if (functionNode.getChildren() == null || functionNode.getChildren().isEmpty()) {
            throw new IllegalArgumentException("percentileOfSeries requires at least one argument: percentiles");
        }

        List<M3ASTNode> children = functionNode.getChildren();
        int argIndex = 0;

        // First arguments: percentiles (required, one or more consecutive numeric values)
        List<Float> percentiles = new ArrayList<>();
        while (argIndex < children.size() && children.get(argIndex) instanceof ValueNode node) {
            String value = node.getValue();
            // Stop if we hit a boolean value (interpolate argument)
            if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                break;
            }
            try {
                float percentile = Float.parseFloat(value);
                if (percentile < 0 || percentile > 100) {
                    // Not a valid percentile, must be a tag
                    break;
                }
                percentiles.add(percentile);
                argIndex++;
            } catch (NumberFormatException e) {
                // Not a number, must be a tag
                break;
            }
        }

        if (percentiles.isEmpty()) {
            throw new IllegalArgumentException("percentileOfSeries requires at least one percentile value");
        }

        // Next argument: interpolate (optional, default false)
        boolean interpolate = false;
        if (argIndex < children.size() && children.get(argIndex) instanceof ValueNode node) {
            String value = node.getValue().toLowerCase(Locale.ROOT);
            if ("true".equals(value) || "false".equals(value)) {
                interpolate = Boolean.parseBoolean(value);
                argIndex++;
            }
        }

        // Remaining arguments: tags (optional)
        List<String> tags = null;
        if (argIndex < children.size()) {
            tags = new ArrayList<>();
            for (int i = argIndex; i < children.size(); i++) {
                if (children.get(i) instanceof ValueNode node) {
                    tags.add(Utils.stripDoubleQuotes(node.getValue()));
                }
            }
        }

        return new PercentileOfSeriesPlanNode(M3PlannerContext.generateId(), percentiles, interpolate, tags);
    }
}
