package com.opendatasoft.elasticsearch.plugin;

import com.opendatasoft.elasticsearch.index.analysis.SynonymReloaderTokenFilterFactory;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.plugins.AbstractPlugin;

public class SynonymReloaderPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "synonym-reloader";
    }

    @Override
    public String description() {
        return "Reload synonym file on change";
    }


    public void onModule(AnalysisModule module) {
        module.addTokenFilter("synonym_reloader", SynonymReloaderTokenFilterFactory.class);
    }

}
