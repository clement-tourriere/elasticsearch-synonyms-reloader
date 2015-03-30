package com.opendatasoft.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.WordnetSynonymParser;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.*;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.*;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

@AnalysisSettingsRequired
public class SynonymReloaderTokenFilterFactory extends AbstractTokenFilterFactory {

    private SynonymMap synonymMap;
    private final boolean ignoreCase;

    @Inject
    public SynonymReloaderTokenFilterFactory(final Index index, @IndexSettings Settings indexSettings, Environment env, IndicesAnalysisService indicesAnalysisService, Map<String, TokenizerFactoryFactory> tokenizerFactories,
                                             @Assisted String name, @Assisted Settings settings, IndicesService indicesService) {
        super(index, indexSettings, name, settings);

        Reader rulesReader = null;
        if (settings.getAsArray("synonyms", null) != null) {
            List<String> rules = Analysis.getWordList(env, settings, "synonyms");
            StringBuilder sb = new StringBuilder();
            for (String line : rules) {
                sb.append(line).append(System.getProperty("line.separator"));
            }
            rulesReader = new FastStringReader(sb.toString());
        } else if (settings.get("synonyms_path") != null) {
            rulesReader = Analysis.getReaderFromFile(env, settings, "synonyms_path");
        } else {
            throw new ElasticsearchIllegalArgumentException("synonym requires either `synonyms` or `synonyms_path` to be configured");
        }

        this.ignoreCase = settings.getAsBoolean("ignore_case", false);
        boolean expand = settings.getAsBoolean("expand", true);

        String tokenizerName = settings.get("tokenizer", "whitespace");

        TokenizerFactoryFactory tokenizerFactoryFactory = tokenizerFactories.get(tokenizerName);
        if (tokenizerFactoryFactory == null) {
            tokenizerFactoryFactory = indicesAnalysisService.tokenizerFactoryFactory(tokenizerName);
        }
        if (tokenizerFactoryFactory == null) {
            throw new ElasticsearchIllegalArgumentException("failed to find tokenizer [" + tokenizerName + "] for synonym token filter");
        }
        final TokenizerFactory tokenizerFactory = tokenizerFactoryFactory.create(tokenizerName, indexSettings);

        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer tokenizer = tokenizerFactory == null ? new WhitespaceTokenizer(reader) : tokenizerFactory.create(reader);
                TokenStream stream = ignoreCase ? new LowerCaseFilter(tokenizer) : tokenizer;
                return new TokenStreamComponents(tokenizer, stream);
            }
        };

        try {
            SynonymMap.Builder parser = null;

            if ("wordnet".equalsIgnoreCase(settings.get("format"))) {
                parser = new WordnetSynonymParser(true, expand, analyzer);
                ((WordnetSynonymParser) parser).parse(rulesReader);
            } else {
                parser = new SolrSynonymParser(true, expand, analyzer);
                ((SolrSynonymParser) parser).parse(rulesReader);
            }

            if (settings.get("synonyms_path") != null) {
                final WatchService watchService = FileSystems.getDefault().newWatchService();
                final Path path = FileSystems.getDefault().getPath(env.resolveConfig(settings.get("synonyms_path")).getPath()).getParent();
                path.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);

                Thread reloader = new Thread(new Reloader(watchService, env, settings, analyzer));

                reloader.start();

//                indicesService.indicesLifecycle().addListener(new IndicesLifecycle.Listener() {
//                    @Override
//                    public void beforeIndexClosed(IndexService indexService) {
//                        if (indexService.index().getName().equals(index.getName())) {
//                            try {
//                                watchService.close();
//                            } catch (IOException e) {
//                                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                            }
//                        }
//                    }
//                });
            }

            synonymMap = parser.build();
        } catch (Exception e) {
            throw new ElasticsearchIllegalArgumentException("failed to build synonyms", e);
        }
    }

    private class Reloader implements Runnable {

        WatchService watchService;
        Environment env;
        Settings settings;
        boolean expand;
        Analyzer analyzer;

        Reloader(WatchService watchService, Environment env, Settings settings, Analyzer analyzer) {
            this.watchService = watchService;
            this.expand = settings.getAsBoolean("expand", true);
            this.analyzer = analyzer;
            this.settings = settings;
            this.env = env;
        }

        @Override
        public void run() {
            while(true) {
                final WatchKey wk;
                try {
                    wk = watchService.take();

                    for (WatchEvent<?> event : wk.pollEvents()) {
                        final Path changed = (Path) event.context();
                        if (changed.endsWith("test_synonym.txt")) {

                            Reader rulesReader = Analysis.getReaderFromFile(env, settings, "synonyms_path");

                            SynonymMap.Builder parser = null;

                            if ("wordnet".equalsIgnoreCase(settings.get("format"))) {
                                parser = new WordnetSynonymParser(true, expand, analyzer);
                                ((WordnetSynonymParser) parser).parse(rulesReader);
                            } else {
                                parser = new SolrSynonymParser(true, expand, analyzer);
                                ((SolrSynonymParser) parser).parse(rulesReader);
                            }

                            synonymMap = parser.build();
                            System.out.println("Synonyms reloaded");
                        }
                    }
                    boolean valid = wk.reset();
                } catch (InterruptedException e) {
                    break;
                } catch (IOException e) {
                    throw new ElasticsearchIllegalArgumentException("failed to build synonyms", e);
                } catch (ParseException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        // fst is null means no synonyms
        return synonymMap.fst == null ? tokenStream : new SynonymFilter(tokenStream, synonymMap, ignoreCase);
    }


}
