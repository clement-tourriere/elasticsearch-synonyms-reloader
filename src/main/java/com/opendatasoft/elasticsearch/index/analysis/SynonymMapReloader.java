package com.opendatasoft.elasticsearch.index.analysis;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.WordnetSynonymParser;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.TokenizerFactoryFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Locale;

public class SynonymMapReloader {

    private volatile SynonymMap synonymMap;
    private final Settings settings;
    private final boolean expand;
    private final Analyzer analyzer;
    private final Environment env;
    private Long lastModified = null;

    public SynonymMapReloader(Environment env, Settings settings, Analyzer analyzer) {
        this.settings = settings;
        this.expand = settings.getAsBoolean("expand", true);
        this.analyzer = analyzer;
        this.env = env;
    }

    public SynonymMap getSynonymMap() {
        if (isModified()) {
            createSynonymMap();
        }
        return synonymMap;
    }

    public void createSynonymMap() {

        Reader rulesReader = getReader();

        try {
            SynonymMap.Builder parser = null;

            if ("wordnet".equalsIgnoreCase(settings.get("format"))) {
                parser = new WordnetSynonymParser(true, expand, analyzer);
                ((WordnetSynonymParser) parser).parse(rulesReader);
            } else {
                parser = new SolrSynonymParser(true, expand, analyzer);
                ((SolrSynonymParser) parser).parse(rulesReader);
            }

            synonymMap = parser.build();
        } catch (Exception e) {
            throw new ElasticsearchIllegalArgumentException("failed to build synonyms", e);
        }
    }

    public boolean isModified() {
        return lastModified == null || getLastModifiedDate() != lastModified;
    }

    private long getLastModifiedDate() {
        try {
            return new File(env.resolveConfig(settings.get("synonyms_path", null)).toURI()).lastModified();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Error");
        }
    }

    private Reader getReader() {
        if (settings.get("synonyms_path") != null) {
            try {
                return Analysis.getReaderFromFile(env, settings, "synonyms_path");
            } catch (Exception e) {
                String message = String.format(Locale.ROOT, "IOException while reading synonyms_path: %s", e.getMessage());
                throw new ElasticsearchIllegalArgumentException(message);
            }
        } else {
            throw new ElasticsearchIllegalArgumentException("file watcher synonym requires `synonyms_path` to be configured");
        }
    }
}
