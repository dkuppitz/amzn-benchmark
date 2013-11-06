/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.loader;

import com.thinkaurelius.amazon.benchmark.Schema;
import com.thinkaurelius.amazon.benchmark.entities.Product;
import com.tinkerpop.blueprints.Vertex;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class ProductTitleLoader extends EntityLoader<Product> {

    private final static Logger logger = Logger.getLogger(ProductTitleLoader.class.getName());
    private final static AtomicLong counter = new AtomicLong(0L);

    @Override
    public void run() {
        final Iterator<Vertex> productItty = this.graph.getVertices(Schema.Keys.PRODUCT_ASIN, this.entity.ASIN).iterator();
        if (productItty.hasNext()) {
            setPropertyIfNotNull(productItty.next(), Schema.Keys.PRODUCT_TITLE, entity.title);
            if (counter.incrementAndGet()%batchSize == 0L) {
                logger.log(Level.INFO, "PRODUCTS :: {0}", counter.get());
            }
        }
        else {
            logger.log(Level.WARNING, "Cannot find product with ASIN ''{0}''", this.entity.ASIN);
        }
    }
}
