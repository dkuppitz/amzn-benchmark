package com.thinkaurelius.amazon.benchmark;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import net.sourceforge.argparse4j.inf.ArgumentParser;

public class Neo4jLoader extends GraphLoader<Neo4jGraph> {

    public Neo4jLoader(final String[] args) throws Exception {
        super(args, "Neo4J graph");
    }

    public static void main(final String[] args) throws Exception
    {
        try (final GraphLoader loader = new Neo4jLoader(args)) {
            loader.load();
        }
        
        System.exit(0);
    }
    
    @Override
    protected void configureArgumentParser(final ArgumentParser parser) {
        super.configureArgumentParser(parser);
        parser.addArgument("--path").help("Neo4J graph db path").required(true).type(String.class);
    }

    @Override
    protected Neo4jGraph createGraphInstance() {
        return new Neo4jGraph((String)this.settings.get("path"));
    }

    @Override
    protected void init() {
        this.graph.createKeyIndex(Schema.Keys.PRODUCT_ASIN, Vertex.class);
        this.graph.createKeyIndex(Schema.Keys.CATEGORY_NAME, Vertex.class);
        this.graph.createKeyIndex(Schema.Keys.USER_ID, Vertex.class);
        this.graph.commit();
    }
}
