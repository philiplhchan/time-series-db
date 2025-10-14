/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A pipeline stage that renames series based on available tag values.
 * Takes multiple tag names and builds an alias from their values, ignoring missing tags.
 */
@PipelineStageAnnotation(name = AliasByTagsStage.NAME)
public class AliasByTagsStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage. */
    public static final String NAME = "aliasByTags";

    private final List<String> tagNames;

    /**
     * Creates a new AliasByTagsStage with the specified tag names.
     *
     * @param tagNames the list of tag names to use for building aliases
     */
    public AliasByTagsStage(List<String> tagNames) {
        this.tagNames = new ArrayList<>(tagNames);
    }

    /**
     * Creates a new AliasByTagsStage by reading from a stream input.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs while reading
     */
    public AliasByTagsStage(final StreamInput in) throws IOException {
        this.tagNames = in.readStringList();
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        List<TimeSeries> result = new ArrayList<>(input.size());
        for (TimeSeries ts : input) {
            String resolvedAlias = buildAliasFromTags(tagNames, ts.getLabels());
            // TODO: is it ok to mutate in place?
            ts = new TimeSeries(ts.getSamples(), ts.getLabels(), ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep(), resolvedAlias);
            result.add(ts);
        }
        return result;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("tag_names", tagNames);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(tagNames);
    }

    /**
     * Create an AliasByTagsStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new AliasByTagsStage instance
     * @throws IOException if an I/O error occurs while reading
     */
    public static AliasByTagsStage readFrom(StreamInput in) throws IOException {
        List<String> tagNames = in.readStringList();
        return new AliasByTagsStage(tagNames);
    }

    /**
     * Creates a new instance of AliasStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct an AliasStage instance.
     *             The map must include a key "pattern" with a String value representing
     *             the alias pattern.
     * @return a new AliasStage instance initialized with the provided alias pattern.
     */
    public static AliasByTagsStage fromArgs(Map<String, Object> args) {
        List<String> tagNames = (List<String>) args.get("tag_names");
        return new AliasByTagsStage(tagNames);
    }

    /**
     * Build alias from available tag values, ignoring missing tags.
     *
     * @param tagNames The list of tag names to use for the alias
     * @param labels The labels map containing tag values
     * @return The constructed alias string
     */
    private String buildAliasFromTags(List<String> tagNames, Labels labels) {
        List<String> foundValues = new ArrayList<>();
        LabelValueProvider labelValueProvider = labels::get;
        if (tagNames.size() > 1) {
            // if we're fetching multiple tags, iterate once and cache as map for efficiency
            Map<String, String> labelsMap = labels.toMapView();
            labelValueProvider = labelsMap::get;
        }
        for (String tagName : tagNames) {
            String tagValue = labelValueProvider.get(tagName);
            if (tagValue != null) {
                foundValues.add(tagValue);
            }
            // If tag not found, ignore it (don't add anything to foundValues)
        }
        return String.join(" ", foundValues);
    }

    @FunctionalInterface
    interface LabelValueProvider {
        String get(String labelName);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AliasByTagsStage that)) return false;
        return Objects.equals(tagNames, that.tagNames);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tagNames);
    }
}
