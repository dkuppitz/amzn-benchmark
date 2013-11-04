/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.entities;

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class CategoryRelations extends AmazonEntity {
    public List<String[]> paths = new LinkedList<>();
}
