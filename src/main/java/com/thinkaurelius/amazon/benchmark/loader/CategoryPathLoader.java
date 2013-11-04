/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.loader;

import com.thinkaurelius.amazon.benchmark.Schema;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class CategoryPathLoader extends VertexLoader<String> {

    private final static Logger logger = Logger.getLogger(CategoryPathLoader.class.getName());
    private final static AtomicLong counter = new AtomicLong(0L);
    
    @Override
    public void run() {
        try {
            final String[] path = this.entity.split(", ");
            
            Vertex parent = null;
            for (final String name : path) {
                final Vertex category;
                if (parent == null) {
                    final Iterator<Vertex> itty = new GremlinPipeline()
                            .start(this.graph.getVertices(Schema.Keys.CATEGORY_NAME, name))
                            .filter(new PipeFunction<Vertex, Boolean>() {
                                @Override
                                public Boolean compute(Vertex v) {
                                    return !v.getEdges(Direction.IN, Schema.Labels.HAS_CATEGORY).iterator().hasNext();
                                }
                            }).cast(Vertex.class).iterator();
                    
                    if (itty.hasNext()) {
                        category = itty.next();
                    }
                    else {
                        category = this.graph.addVertex(null);
                        category.setProperty(Schema.Keys.CATEGORY_NAME, name);
                        if (counter.incrementAndGet()%batchSize == 0L) {
                            logger.log(Level.INFO, "CATEGORIES :: {0}", counter.get());
                        }
                    }
                }
                else {
                    final Iterator<Vertex> itty = new GremlinPipeline(parent)
                            .out(Schema.Labels.HAS_CATEGORY)
                            .has(Schema.Keys.CATEGORY_NAME, name)
                            .cast(Vertex.class).iterator();
                    
                    if (itty.hasNext()) {
                        category = itty.next();
                    }
                    else {
                        category = this.graph.addVertex(null);
                        category.setProperty(Schema.Keys.CATEGORY_NAME, name);
                        parent.addEdge(Schema.Labels.HAS_CATEGORY, category);
                        if (counter.incrementAndGet()%batchSize == 0L) {
                            logger.log(Level.INFO, "CATEGORIES :: {0}", counter.get());
                        }
                    }
                }
                parent = category;
            }
        }
        finally {
            this.loader.notifyEntityDone();
        }
    }
}
