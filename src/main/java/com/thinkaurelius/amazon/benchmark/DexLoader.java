package com.thinkaurelius.amazon.benchmark;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.dex.DexGraph;
import net.sourceforge.argparse4j.inf.ArgumentParser;

public class DexLoader extends GraphLoader<DexGraph> {

    public DexLoader(final String[] args) throws Exception {
        super(args, "Dex graph");
    }

    public static void main(final String[] args) throws Exception
    {
        try (final GraphLoader loader = new DexLoader(args)) {
            loader.load();
        }
        
        System.exit(0);
    }
    
    @Override
    protected void configureArgumentParser(final ArgumentParser parser) {
        super.configureArgumentParser(parser);
        parser.addArgument("--path").help("Dex graph db path").required(true).type(String.class);
    }

    @Override
    protected DexGraph createGraphInstance() {
        return new DexGraph((String)this.settings.get("path"));
    }

    @Override
    protected void init() {
        this.graph.createKeyIndex(Schema.Keys.PRODUCT_ASIN, Vertex.class);
        this.graph.createKeyIndex(Schema.Keys.CATEGORY_NAME, Vertex.class);
        this.graph.createKeyIndex(Schema.Keys.USER_ID, Vertex.class);
        this.graph.commit();
    }
}
