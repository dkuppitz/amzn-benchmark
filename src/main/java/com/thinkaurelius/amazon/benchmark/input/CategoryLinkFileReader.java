/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.input;

import com.thinkaurelius.amazon.benchmark.entities.CategoryRelations;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class CategoryLinkFileReader extends InputFileReader<CategoryRelations> {

    public CategoryLinkFileReader(final String filename) throws FileNotFoundException, IOException {
        super(filename);
    }

    @Override
    public CategoryRelations next() {
        if (this.hasNext()) {
            final CategoryRelations relations = new CategoryRelations();
            relations.ASIN = this.nextLine;
            hasRead = false;
            while (true) {
                if (this.hasNext() && this.nextLine.startsWith("  ")) {
                    relations.paths.add(this.nextLine.substring(2).split(", "));
                    hasRead = false;
                }
                else break;
            }
            return relations;
        }
        else {
            throw new RuntimeException("Cannot call next() on empty iterator.");
        }
    }
}
