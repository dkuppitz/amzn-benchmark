package com.thinkaurelius.amazon.benchmark;

import com.thinkaurelius.titan.core.KeyMaker;
import com.thinkaurelius.titan.core.LabelMaker;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanType;
import com.tinkerpop.blueprints.Vertex;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class TitanLoader extends GraphLoader<TitanGraph> {

    public TitanLoader(final String[] args) throws Exception {
        super(args, "Titan graph");
    }

    public static void main(final String[] args) throws Exception
    {
        try (final GraphLoader loader = new TitanLoader(args)) {
            loader.load();
        }
        
        System.exit(0);
    }
    
    @Override
    protected void configureArgumentParser(final ArgumentParser parser) {
        super.configureArgumentParser(parser);
        parser.addArgument("-i", "--index").help("name of the configured ES index").required(false).type(String.class);
        parser.addArgument("--config").help("graph configuration file").setDefault("./config/cassandra.properties").type(String.class);
        parser.addArgument("--sorted").help("whether a sort key on '" + Schema.Keys.REVIEW_SCORE + "' is created or not").nargs("?").setConst(true).setDefault(false);
    }

    @Override
    protected TitanGraph createGraphInstance() throws ConfigurationException {
        final Configuration config = new PropertiesConfiguration((String)this.settings.get("config"));
        return TitanFactory.open(config);
    }

    @Override
    protected void init() {
        
        final Boolean sorted = (Boolean)this.settings.get("sorted");
        final String index = (String)this.settings.get("index");

        TitanType score = this.graph.getType(Schema.Keys.REVIEW_SCORE);

        if (this.graph.getType(Schema.Keys.PRODUCT_ASIN) == null) {
            this.graph.makeKey(Schema.Keys.PRODUCT_ASIN).dataType(String.class).single().indexed(Vertex.class).unique().make();
        }
        if (this.graph.getType(Schema.Keys.PRODUCT_TITLE) == null) {
            final KeyMaker productTitle = this.graph.makeKey(Schema.Keys.PRODUCT_TITLE).dataType(String.class).single();
            if (index != null) {
                productTitle.indexed(index, Vertex.class);
            }
            productTitle.make();
        }
        if (this.graph.getType(Schema.Keys.USER_ID) == null) {
            this.graph.makeKey(Schema.Keys.USER_ID).dataType(String.class).single().indexed(Vertex.class).unique().make();
        }
        if (this.graph.getType(Schema.Keys.CATEGORY_NAME) == null) {
            this.graph.makeKey(Schema.Keys.CATEGORY_NAME).dataType(String.class).single().indexed(Vertex.class).make();
        }
        if (this.graph.getType(Schema.Keys.USER_NAME) == null) {
            final KeyMaker userName = this.graph.makeKey(Schema.Keys.USER_NAME).dataType(String.class).single();
            if (index != null) {
                userName.indexed(index, Vertex.class);
            }
            userName.make();
        }
        if (this.graph.getType(Schema.Keys.REVIEW_TIME) == null) {
            this.graph.makeKey(Schema.Keys.REVIEW_TIME).dataType(Long.class).single().make();
        }
        if (score == null) {
            score = this.graph.makeKey(Schema.Keys.REVIEW_SCORE).dataType(Float.class).single().make();
        }
        if (this.graph.getType(Schema.Keys.REVIEW_HELPFULNESS) == null) {
            this.graph.makeKey(Schema.Keys.REVIEW_HELPFULNESS).dataType(Float.class).single().make();
        }
        if (this.graph.getType(Schema.Keys.REVIEW_SUMMARY) == null) {
            final KeyMaker reviewedSummary = this.graph.makeKey(Schema.Keys.REVIEW_SUMMARY).dataType(String.class).single();
            if (index != null) {
                reviewedSummary.indexed(index, Vertex.class);
            }
            reviewedSummary.make();
        }
        if (this.graph.getType(Schema.Keys.REVIEW_TEXT) == null) {
            final KeyMaker reviewedText = this.graph.makeKey(Schema.Keys.REVIEW_TEXT).dataType(String.class).single();
            if (index != null) {
                reviewedText.indexed(index, Vertex.class);
            }
            reviewedText.make();
        }
        if (this.graph.getType(Schema.Labels.HAS_CATEGORY) == null) {
            this.graph.makeLabel(Schema.Labels.HAS_CATEGORY).make();
        }
        if (this.graph.getType(Schema.Labels.HAS_PRODUCT) == null) {
            this.graph.makeLabel(Schema.Labels.HAS_PRODUCT).make();
        }
        if (this.graph.getType(Schema.Labels.REVIEWED) == null) {
            final LabelMaker reviewed = this.graph.makeLabel(Schema.Labels.REVIEWED);
            if (sorted) reviewed.sortKey(score);
            reviewed.make();
        }
        this.graph.commit();
    }
}
