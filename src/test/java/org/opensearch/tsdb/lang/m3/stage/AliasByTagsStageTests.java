/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class AliasByTagsStageTests extends AbstractWireSerializingTestCase<AliasByTagsStage> {

    public void testSingleSeriesWithSingleTag() {
        AliasByTagsStage stage = new AliasByTagsStage(List.of("city"));

        List<Sample> samples = List.of(new FloatSample(10L, 10.0), new FloatSample(20L, 20.0), new FloatSample(30L, 30.0));
        ByteLabels labels = ByteLabels.fromStrings("city", "atlanta", "name", "actions");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 30L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("atlanta", result.get(0).getAlias());
    }

    public void testMultipleSeriesWithSingleTag() {
        AliasByTagsStage stage = new AliasByTagsStage(List.of("city"));

        List<Sample> samples1 = List.of(new FloatSample(10L, 10.0), new FloatSample(20L, 20.0), new FloatSample(30L, 30.0));
        List<Sample> samples2 = List.of(new FloatSample(1L, 1.0), new FloatSample(2L, 2.0), new FloatSample(3L, 3.0));

        ByteLabels labels1 = ByteLabels.fromStrings("city", "atlanta", "name", "actions");
        ByteLabels labels2 = ByteLabels.fromStrings("city", "boston", "name", "cities");

        TimeSeries ts1 = new TimeSeries(samples1, labels1, 10L, 30L, 10L, null);
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 1L, 3L, 1L, null);

        List<TimeSeries> result = stage.process(List.of(ts1, ts2));

        assertEquals(2, result.size());
        assertEquals("atlanta", result.get(0).getAlias());
        assertEquals("boston", result.get(1).getAlias());
    }

    public void testMultipleSeriesWithMultipleTags() {
        AliasByTagsStage stage = new AliasByTagsStage(List.of("city", "dc"));

        List<Sample> samples1 = List.of(new FloatSample(10L, 10.0), new FloatSample(20L, 20.0), new FloatSample(30L, 30.0));
        List<Sample> samples2 = List.of(new FloatSample(1L, 1.0), new FloatSample(2L, 2.0), new FloatSample(3L, 3.0));

        ByteLabels labels1 = ByteLabels.fromStrings("city", "atlanta", "dc", "dca1", "name", "actions");
        ByteLabels labels2 = ByteLabels.fromStrings("city", "boston", "dc", "phx1", "name", "cities");

        TimeSeries ts1 = new TimeSeries(samples1, labels1, 10L, 30L, 10L, null);
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 1L, 3L, 1L, null);

        List<TimeSeries> result = stage.process(List.of(ts1, ts2));

        assertEquals(2, result.size());
        assertEquals("atlanta dca1", result.get(0).getAlias());
        assertEquals("boston phx1", result.get(1).getAlias());
    }

    public void testWithMissingTags() {
        AliasByTagsStage stage = new AliasByTagsStage(List.of("city", "missing", "dc"));

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("city", "atlanta", "dc", "dca1", "name", "actions");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("atlanta dca1", result.get(0).getAlias());
    }

    public void testWithEmptyInput() {
        AliasByTagsStage stage = new AliasByTagsStage(List.of("city"));
        List<TimeSeries> result = stage.process(List.of());
        assertTrue(result.isEmpty());
    }

    public void testGetName() {
        AliasByTagsStage stage = new AliasByTagsStage(List.of("city"));
        assertEquals("aliasByTags", stage.getName());
    }

    public void testFromArgs() {
        Map<String, Object> args = Map.of("tag_names", List.of("city", "dc"));
        AliasByTagsStage stage = AliasByTagsStage.fromArgs(args);

        List<Sample> samples = List.of(new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("city", "atlanta", "dc", "dca1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));
        assertEquals("atlanta dca1", result.get(0).getAlias());
    }

    public void testPipelineStageFactory() {
        assertTrue(PipelineStageFactory.isStageTypeSupported(AliasByTagsStage.NAME));
        PipelineStage stage = PipelineStageFactory.createWithArgs(AliasByTagsStage.NAME, Map.of("tag_names", List.of("city", "dc")));
        assertTrue(stage instanceof AliasByTagsStage);
    }

    /**
     * Basic test for PipelineStageFactory.readFrom(StreamInput)
     */
    public void testPipelineStageFactoryReadFrom_StreamInput() throws Exception {
        AliasByTagsStage original = new AliasByTagsStage(List.of("instance"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Factory variant that reads stage name first
            out.writeString(AliasByTagsStage.NAME);
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                PipelineStage restored = PipelineStageFactory.readFrom(in);
                assertNotNull(restored);
                assertTrue(restored instanceof AliasByTagsStage);
            }
        }
    }

    public void testToXContext() throws IOException {
        AliasByTagsStage stage = new AliasByTagsStage(List.of("city", "dc"));
        try (XContentBuilder builder = XContentFactory.jsonBuilder().startObject()) {
            stage.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();

            String json = builder.toString();
            assertEquals("{\"tag_names\":[\"city\",\"dc\"]}", json);
        }
    }

    @Override
    protected AliasByTagsStage createTestInstance() {
        int numRefs = randomIntBetween(0, 3);
        List<String> tagNames = new ArrayList<>();
        for (int i = 0; i < numRefs; i++) {
            tagNames.add(randomAlphaOfLength(5));
        }
        return new AliasByTagsStage(tagNames);
    }

    @Override
    protected Writeable.Reader<AliasByTagsStage> instanceReader() {
        return AliasByTagsStage::new;
    }
}
