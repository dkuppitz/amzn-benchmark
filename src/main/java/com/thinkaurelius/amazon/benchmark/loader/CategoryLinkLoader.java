/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.loader;

import com.thinkaurelius.amazon.benchmark.Schema;
import com.thinkaurelius.amazon.benchmark.entities.CategoryRelations;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class CategoryLinkLoader extends EntityLoader<CategoryRelations> {

    private final static Logger logger = Logger.getLogger(CategoryLinkLoader.class.getName());
    private final static AtomicLong counter = new AtomicLong(0L);
    private final static CategoryCache categoryCache = new CategoryCache();

    @Override
    public void run() {
        try {
            final Iterator<Vertex> productItty = this.graph.getVertices(Schema.Keys.PRODUCT_ASIN, entity.ASIN).iterator();
            if (productItty.hasNext()) {
                final Vertex product = productItty.next();
                for (final String[] path : entity.paths) {
                    CategoryCache parent = categoryCache;
                    for (final String name : path) {
                        parent = parent.getOrCreateChildCategory(graph, name);
                    }
                    graph.getVertex(parent.getVertexId()).addEdge(Schema.Labels.HAS_PRODUCT, product);
                }
                if (counter.incrementAndGet()%batchSize == 0L) {
                    logger.log(Level.INFO, "CATEGORY LINKS :: {0}", counter.get());
                }
            }
        }
        finally {
            this.loader.notifyEntityDone();
        }
    }

    static class CategoryCache {
    
        private final Object id;
        private final Map<String, CategoryCache> children;
        private final Object lock = new Object();

        public CategoryCache() {
            this(null);
        }

        private CategoryCache(final Object id) {
            this.id = id;
            this.children = new HashMap<>();
        }

        public CategoryCache getOrCreateChildCategory(final Graph graph, final String name) {
            if (!children.containsKey(name)) {
                synchronized (lock) {
                    if (!children.containsKey(name)) {
                        final Vertex category;
                        if (this.id == null) {
                            category = (Vertex)new GremlinPipeline()
                                    .start(graph.getVertices(Schema.Keys.CATEGORY_NAME, name))
                                    .filter(new PipeFunction<Vertex, Boolean>() {
                                        @Override
                                        public Boolean compute(Vertex v) {
                                            return !v.getEdges(Direction.IN, Schema.Labels.HAS_CATEGORY).iterator().hasNext();
                                        }
                                    }).next();
                        }
                        else {
                            category = (Vertex)new GremlinPipeline(graph.getVertex(this.id))
                                    .out(Schema.Labels.HAS_CATEGORY)
                                    .has(Schema.Keys.CATEGORY_NAME, name).next();
                        }
                        children.put(name, new CategoryCache(category.getId()));
                    }
                }
            }
            return children.get(name);
        }

        public Object getVertexId() {
            return this.id;
        }
    }
}