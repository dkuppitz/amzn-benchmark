package com.thinkaurelius.amazon.benchmark;

import com.thinkaurelius.amazon.benchmark.input.CategoryLinkFileReader;
import com.thinkaurelius.amazon.benchmark.input.KeyFileReader;
import com.thinkaurelius.amazon.benchmark.input.ProductReviewFileReader;
import com.thinkaurelius.amazon.benchmark.input.ProductTitleFileReader;
import com.thinkaurelius.amazon.benchmark.loader.CategoryLinkLoader;
import com.thinkaurelius.amazon.benchmark.loader.CategoryPathLoader;
import com.thinkaurelius.amazon.benchmark.loader.KeyLoader;
import com.thinkaurelius.amazon.benchmark.loader.ProductReviewLoader;
import com.thinkaurelius.amazon.benchmark.loader.ProductTitleLoader;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.dex.DexGraph;
import java.io.IOException;
import java.util.logging.Level;
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
        this.graph.label.set("product");
        this.graph.createKeyIndex(Schema.Keys.PRODUCT_ASIN, Vertex.class);
        this.graph.label.set("category");
        this.graph.createKeyIndex(Schema.Keys.CATEGORY_NAME, Vertex.class);
        this.graph.label.set("user");
        this.graph.createKeyIndex(Schema.Keys.USER_ID, Vertex.class);
        this.graph.commit();
    }
    
    @Override
    protected void loadASINs() {
        this.graph.label.set("product");
        super.loadASINs();
    }

    @Override
    protected void loadUserIds() {
        this.graph.label.set("user");
        super.loadUserIds();
    }
    
    @Override
    protected void loadCategoryPaths() {
        this.graph.label.set("category");
        super.loadCategoryPaths();
    }
}
