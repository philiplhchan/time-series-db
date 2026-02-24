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
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A pipeline stage that generates mock time series data based on provided values and tags.
 *
 * MockFetchStage generates synthetic time series data on the coordinator node for testing purposes.
 * Unlike stages that transform existing time series data, MockFetchStage ignores its input and creates
 * new data from scratch based on the provided values.
 *
 */
@PipelineStageAnnotation(name = MockFetchStage.NAME)
public class MockFetchStage implements UnaryPipelineStage {

    public static final String NAME = "mockFetch";

    private final List<Double> values;
    private final Map<String, String> tags;
    private final long startTime;
    private final long step;

    /**
     * Constructor for MockFetchStage.
     *
     * @param values List of values to generate
     * @param tags Map of tag key-value pairs for the series
     * @param startTime Start timestamp in milliseconds
     * @param step Step size in milliseconds
     */
    public MockFetchStage(List<Double> values, Map<String, String> tags, long startTime, long step) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("MockFetch requires at least one value");
        }
        this.values = new ArrayList<>(values);
        this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
        this.startTime = startTime;
        this.step = step;

        // Add default tag if no tags provided
        if (this.tags.isEmpty()) {
            this.tags.put("name", "mockFetch");
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Process method for PipelineStage interface.
     * MockFetchStage generates new time series data rather than transforming existing data.
     * The input parameter is ignored.
     *
     * @param input ignored (can be null or empty)
     * @return generated time series list with one series
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (step == 0) {
            throw new IllegalStateException("MockFetch stage requires setQueryContext() to be called before process()");
        }

        FloatSampleList.Builder builder = new FloatSampleList.Builder(values.size());
        for (int i = 0; i < values.size(); i++) {
            long timestamp = startTime + (i * step);
            double value = values.get(i);
            // Skip missing samples
            if (!Double.isNaN(value)) {
                builder.add(timestamp, value);
            }
        }

        long endTime = startTime + ((values.size() - 1) * step);
        Labels labels = ByteLabels.fromMap(tags);
        SampleList samples = builder.build();

        TimeSeries series = new TimeSeries(samples, labels, startTime, endTime, step, null);

        return List.of(series);
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("values", values);
        builder.field("tags", tags);
        builder.field("startTime", startTime);
        builder.field("step", step);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(values.size());
        for (Double value : values) {
            out.writeDouble(value);
        }
        out.writeMap(tags, StreamOutput::writeString, StreamOutput::writeString);
        out.writeVLong(startTime);
        out.writeVLong(step);
    }

    /**
     * Create a MockFetchStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new MockFetchStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static MockFetchStage readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<Double> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(in.readDouble());
        }
        Map<String, String> tags = in.readMap(StreamInput::readString, StreamInput::readString);
        long startTime = in.readVLong();
        long step = in.readVLong();
        return new MockFetchStage(values, tags, startTime, step);
    }

    /**
     * Create a MockFetchStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return MockFetchStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    @SuppressWarnings("unchecked")
    public static MockFetchStage fromArgs(Map<String, Object> args) {
        if (!args.containsKey("values")) {
            throw new IllegalArgumentException("MockFetch requires 'values' argument");
        }

        Object valuesObj = args.get("values");
        List<Double> values = new ArrayList<>();

        if (valuesObj instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Number num) {
                    values.add(num.doubleValue());
                } else if (item instanceof String str) {
                    try {
                        values.add(Double.parseDouble(str));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid numeric value: " + str, e);
                    }
                } else {
                    throw new IllegalArgumentException("Invalid value type: " + item.getClass());
                }
            }
        } else if (valuesObj instanceof Number num) {
            values.add(num.doubleValue());
        } else if (valuesObj instanceof String str) {
            try {
                values.add(Double.parseDouble(str));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid numeric value: " + str, e);
            }
        } else {
            throw new IllegalArgumentException("Invalid values argument type: " + valuesObj.getClass());
        }

        Map<String, String> tags = new HashMap<>();
        if (args.containsKey("tags") && args.get("tags") instanceof Map<?, ?> tagsMap) {
            for (Map.Entry<?, ?> entry : tagsMap.entrySet()) {
                tags.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
        }

        if (tags.isEmpty()) {
            tags.put("name", "mockFetch");
        }

        // Extract startTime and step if provided, otherwise use defaults (0)
        long startTime = 0;
        long step = 0;
        if (args.containsKey("startTime") && args.get("startTime") instanceof Number num) {
            startTime = num.longValue();
        }
        if (args.containsKey("step") && args.get("step") instanceof Number num) {
            step = num.longValue();
        }

        return new MockFetchStage(values, tags, startTime, step);
    }

    /**
     * Returns the values for testing purposes.
     * @return list of values
     */
    public List<Double> getValues() {
        return new ArrayList<>(values);
    }

    /**
     * Returns the tags for testing purposes.
     * @return map of tags
     */
    public Map<String, String> getTags() {
        return new HashMap<>(tags);
    }

    @Override
    public boolean isCoordinatorOnly() {
        return true; // MockFetch must run on coordinator since it doesn't fetch from shards
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MockFetchStage that = (MockFetchStage) obj;
        return Objects.equals(values, that.values) && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values, tags);
    }
}
