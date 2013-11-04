/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.loader;

import com.thinkaurelius.amazon.benchmark.entities.AmazonEntity;
import com.tinkerpop.blueprints.Element;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
//public abstract class EntityLoader<T extends AmazonEntity> implements Runnable {
public abstract class EntityLoader<T extends AmazonEntity> extends VertexLoader<T> {
    
    protected static void setPropertyIfNotNull(final Element element, final String key, final Object value) {
        if (value != null) {
            element.setProperty(key, value);
        }
    }
}
