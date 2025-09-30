/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A pipeline stage that renames series in a series list.
 * Supports tag value interpolation using {{.tag}} syntax.
 */
@PipelineStageAnnotation(name = AliasStage.NAME)
public class AliasStage implements UnaryPipelineStage {
    /** The name of this pipeline stage. */
    public static final String NAME = "alias";

    private static final Pattern TAG_INTERPOLATION_PATTERN = Pattern.compile("\\{\\{\\.(\\w+)\\}\\}");
    @Nullable
    private final String aliasPattern;

    /**
     * Constructs a new AliasStage with the specified alias pattern.
     *
     * @param aliasPattern the alias pattern to use for renaming series
     */
    public AliasStage(@Nullable String aliasPattern) {
        this.aliasPattern = aliasPattern;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        for (TimeSeries ts : input) {
            String resolvedAlias = resolveAliasPattern(aliasPattern, ts.getLabelsMap());
            // TODO: should this create a copy of the time series? or ok to mutate in place?
            ts.setAlias(resolvedAlias);
        }
        return input;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("pattern", aliasPattern);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(aliasPattern);
    }

    /**
     * Create an AliasStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new AliasStage instance
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public static AliasStage readFrom(StreamInput in) throws IOException {
        String aliasPattern = in.readString();
        return new AliasStage(aliasPattern);
    }

    /**
     * Creates a new instance of AliasStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct an AliasStage instance.
     *             The map must include a key "pattern" with a String value representing
     *             the alias pattern.
     * @return a new AliasStage instance initialized with the provided alias pattern.
     */
    public static AliasStage fromArgs(Map<String, Object> args) {
        String aliasPattern = (String) args.get("pattern");
        return new AliasStage(aliasPattern);
    }

    /**
     * Get the alias pattern.
     * @return the alias pattern
     */
    @Nullable
    String getAliasPattern() {
        return aliasPattern;
    }

    /**
     * Resolve the alias pattern by replacing {{.tag}} placeholders with actual tag values.
     * If a tag is missing, replace with the tag name itself (without {{.}} wrapper).
     *
     * @param pattern The alias pattern containing potential {{.tag}} placeholders
     * @param labels The labels map containing tag values
     * @return The resolved alias name
     */
    private String resolveAliasPattern(String pattern, Map<String, String> labels) {
        if (pattern == null) {
            return null;
        }

        Matcher matcher = TAG_INTERPOLATION_PATTERN.matcher(pattern);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String tagName = matcher.group(1);
            String tagValue = labels.get(tagName);
            if (tagValue != null) {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(tagValue));
            } else {
                // If tag is not found, replace with the tag name itself
                matcher.appendReplacement(sb, Matcher.quoteReplacement(tagName));
            }
        }
        matcher.appendTail(sb);

        return sb.toString();
    }
}
