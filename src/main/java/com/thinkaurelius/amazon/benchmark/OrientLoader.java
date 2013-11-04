package com.thinkaurelius.amazon.benchmark;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import net.sourceforge.argparse4j.inf.ArgumentParser;

public class OrientLoader extends GraphLoader<OrientGraph> {

    public OrientLoader(final String[] args) throws Exception {
        super(args, "OrientDB graph");
    }

    public static void main(final String[] args) throws Exception
    {
        try (final GraphLoader loader = new OrientLoader(args)) {
            loader.load();
        }
        
        System.exit(0);
    }
    
    @Override
    protected void configureArgumentParser(final ArgumentParser parser) {
        super.configureArgumentParser(parser);
        parser.addArgument("--path").help("OrientDB graph db path").required(true).type(String.class);
    }

    @Override
    protected OrientGraph createGraphInstance() {
        String path = (String)this.settings.get("path");
        if (!path.startsWith("local:") && !path.startsWith("remote:") && !path.startsWith("memory:")) {
            path = "local:" + path;
        }
        return new OrientGraph(path);
    }

    @Override
    protected void init() {
        this.graph.createKeyIndex(Schema.Keys.PRODUCT_ASIN, Vertex.class);
        this.graph.createKeyIndex(Schema.Keys.CATEGORY_NAME, Vertex.class);
        this.graph.createKeyIndex(Schema.Keys.USER_ID, Vertex.class);
        this.graph.commit();
    }
    
    @Override
    protected void commit() {
        this.graph.commit();
    }
}
