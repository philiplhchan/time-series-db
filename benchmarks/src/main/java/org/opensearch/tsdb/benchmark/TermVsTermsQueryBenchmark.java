/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Server-side benchmark comparing term query vs terms query performance.
 * This benchmark tests the actual Lucene query execution without HTTP client overhead.
 *
 * Parameterized by:
 * - termCount: Number of terms to search (1, 10, 100)
 * - useTermsQuery: true = TermInSetQuery, false = BooleanQuery with SHOULD clauses
 */
@Fork(value = 1)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class TermVsTermsQueryBenchmark {

    private static final String FIELD_NAME = "labels";
    private static final int TOTAL_DOC_COUNT = 10000;
    private static final int UNIQUE_TERMS = 100;  // Generate 100 unique terms for indexing

    // Generate term values dynamically
    private static final String[] ALL_TERMS = new String[UNIQUE_TERMS];
    static {
        for (int i = 0; i < UNIQUE_TERMS; i++) {
            ALL_TERMS[i] = "service:" + i;
        }
    }

    /**
     * Number of terms to include in the query
     */
    @Param({ "1", "10", "100" })
    public int termCount;

    /**
     * Whether to use TermInSetQuery (true) or BooleanQuery with SHOULD (false)
     */
    @Param({ "true", "false" })
    public boolean useTermsQuery;

    // Index infrastructure
    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;

    // Current query (built based on parameters)
    private Query query;
    private String[] selectedTerms;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        // Create in-memory directory
        directory = new ByteBuffersDirectory();

        // Create index with documents
        IndexWriterConfig config = new IndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            // Index documents with various label values
            for (int i = 0; i < TOTAL_DOC_COUNT; i++) {
                Document doc = new Document();

                // Distribute documents evenly across all terms
                String labelValue = ALL_TERMS[i % UNIQUE_TERMS];
                doc.add(new KeywordField(FIELD_NAME, labelValue, Field.Store.NO));

                writer.addDocument(doc);
            }
            writer.commit();
        }

        // Open reader and searcher
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);

        // Select first N terms for the query
        selectedTerms = new String[termCount];
        System.arraycopy(ALL_TERMS, 0, selectedTerms, 0, termCount);

        // Build query based on parameters
        buildQuery();
        printBenchmarkInfo();
    }

    private void printBenchmarkInfo() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("SERVER-SIDE BENCHMARK CONFIGURATION");
        System.out.println("=".repeat(80));
        System.out.println("Total documents: " + reader.numDocs());
        System.out.println("Unique terms: " + UNIQUE_TERMS);
        System.out.println("Terms in query: " + termCount);
        System.out.println("Expected matches: ~" + (TOTAL_DOC_COUNT * termCount / UNIQUE_TERMS));
        System.out.println("Query type: " + (useTermsQuery ? "TermInSetQuery" : "BooleanQuery SHOULD"));
        System.out.println("Query class: " + query.getClass().getSimpleName());
        System.out.println("=".repeat(80) + "\n\n");
    }

    private void buildQuery() {
        if (useTermsQuery) {
            // Use TermInSetQuery
            List<BytesRef> termsList = new ArrayList<>();
            for (String term : selectedTerms) {
                termsList.add(new BytesRef(term));
            }
            query = new TermInSetQuery(FIELD_NAME, termsList);
        } else {
            // Use BooleanQuery with SHOULD clauses
            if (termCount == 1) {
                // Special case: single term - use TermQuery directly instead of BooleanQuery
                query = new TermQuery(new Term(FIELD_NAME, selectedTerms[0]));
            } else {
                BooleanQuery.Builder boolBuilder = new BooleanQuery.Builder();
                for (String term : selectedTerms) {
                    boolBuilder.add(new TermQuery(new Term(FIELD_NAME, term)), BooleanClause.Occur.SHOULD);
                }
                query = boolBuilder.build();
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (directory != null) {
            directory.close();
        }
    }

    /**
     * Benchmark: Execute search and collect all matching documents
     *
     * Search phases included:
     * - Query execution (term lookup, posting list iteration)
     * - Document collection (TopScoreDocCollector)
     * - Scoring (BM25 calculation for each match)
     * - Priority queue management (heap operations for top-K)
     * - Final result sorting
     *
     * This simulates a typical search request that returns top matching documents.
     */
    @Benchmark
    public void benchmarkSearch(Blackhole bh) throws IOException {
        bh.consume(searcher.search(query, TOTAL_DOC_COUNT));
    }

    /**
     * Benchmark: Count matching documents without collecting them
     *
     * Search phases included:
     * - Query execution (term lookup, posting list iteration)
     *
     * Search phases NOT included:
     * - Document collection
     * - Scoring
     * - Sorting
     *
     * This isolates pure query performance. Uses optimized Weight.count() when possible,
     * which can use index statistics instead of iterating all matches.
     * Best for measuring raw query algorithm differences.
     */
    @Benchmark
    public void benchmarkCount(Blackhole bh) throws IOException {
        bh.consume(searcher.count(query));
    }

    /**
     * Benchmark: Collect document IDs with TotalHitCountCollector
     *
     * Search phases included:
     * - Query execution (term lookup, posting list iteration)
     * - Document collection (minimal - just counter increment per match)
     *
     * Search phases NOT included:
     * - Scoring (ScoreMode.COMPLETE_NO_SCORES)
     * - Sorting
     * - Priority queue management
     *
     * This simulates aggregation-like workload where we need to visit all matches
     * but don't need scores or top-K results. More realistic than count() for
     * measuring aggregation performance, but still very lightweight.
     */
    @Benchmark
    public void benchmarkTotalHitCollector(Blackhole bh) throws IOException {
        TotalHitCountCollector collector = new TotalHitCountCollector();
        searcher.search(query, collector);
        bh.consume(collector.getTotalHits());
    }

    /**
     * Benchmark: Collect with empty/no-op collector
     *
     * Search phases included:
     * - Query execution (term lookup, posting list iteration)
     * - Document collection loop (but does nothing per match)
     *
     * Search phases NOT included:
     * - Scoring (ScoreMode.COMPLETE_NO_SCORES)
     * - Any per-document work
     * - Sorting
     *
     * This is an artificial baseline - not useful for real workloads.
     * Shows absolute minimum overhead of the collector framework.
     * Use count() instead for pure query performance.
     */
    @Benchmark
    public void benchmarkNoOpCollector(Blackhole bh) throws IOException {
        searcher.search(query, new SimpleCollector() {
            @Override
            public void collect(int doc) {
                // Intentionally empty - just iterate, don't collect anything
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }

            @Override
            protected void doSetNextReader(LeafReaderContext context) {
                // No-op
            }
        });
        bh.consume(1);  // Consume something to prevent dead code elimination
    }
}
