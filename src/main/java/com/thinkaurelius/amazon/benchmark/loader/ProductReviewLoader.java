/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.loader;

import com.thinkaurelius.amazon.benchmark.Schema;
import com.thinkaurelius.amazon.benchmark.entities.ProductReview;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class ProductReviewLoader extends EntityLoader<ProductReview> {

    private final static Logger logger = Logger.getLogger(ProductReviewLoader.class.getName());
    private final static AtomicLong counter = new AtomicLong(0L);

    @Override
    public void run() {
        if (entity.user.userId != null) {
            try {
                final Iterator<Vertex> userItty = this.graph.getVertices(Schema.Keys.USER_ID, entity.user.userId).iterator();
                final Iterator<Vertex> productItty = this.graph.getVertices(Schema.Keys.PRODUCT_ASIN, entity.ASIN).iterator();
                if (userItty.hasNext() && productItty.hasNext()) {
                    final Edge reviewed = userItty.next().addEdge(Schema.Labels.REVIEWED, productItty.next());
                    setPropertyIfNotNull(reviewed, Schema.Keys.REVIEW_TIME, entity.time);
                    setPropertyIfNotNull(reviewed, Schema.Keys.REVIEW_HELPFULNESS, entity.helpfulness);
                    setPropertyIfNotNull(reviewed, Schema.Keys.REVIEW_SCORE, entity.score);
                    setPropertyIfNotNull(reviewed, Schema.Keys.REVIEW_SUMMARY, entity.summary);
                    setPropertyIfNotNull(reviewed, Schema.Keys.REVIEW_TEXT, entity.text);
                    if (counter.incrementAndGet()%batchSize == 0L) {
                        logger.log(Level.INFO, "REVIEWS :: {0}", counter.get());
                    }
                }
            }
            finally {
                this.loader.notifyEntityDone();
            }
        }
    }
}
