package com.thinkaurelius.amazon.benchmark;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import org.apache.commons.configuration.ConfigurationException;

public class TinkerGraphLoader extends GraphLoader<TinkerGraph> {

    public TinkerGraphLoader(final String[] args) throws Exception {
        super(args, "TinkerGraph");
    }

    public static void main(final String[] args) throws Exception
    {
        try (final GraphLoader loader = new TinkerGraphLoader(args)) {
            loader.load();
        }
        
        System.exit(0);
    }
    
    @Override
    protected TinkerGraph createGraphInstance() throws ConfigurationException {
        return new TinkerGraph();
    }

    @Override
    protected void init() {
        this.graph.createKeyIndex(Schema.Keys.PRODUCT_ASIN, Vertex.class);
        this.graph.createKeyIndex(Schema.Keys.CATEGORY_NAME, Vertex.class);
        this.graph.createKeyIndex(Schema.Keys.USER_ID, Vertex.class);
    }
}
