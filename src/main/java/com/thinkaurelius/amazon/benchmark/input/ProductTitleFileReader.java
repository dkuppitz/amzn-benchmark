/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.input;

import com.thinkaurelius.amazon.benchmark.entities.Product;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class ProductTitleFileReader extends InputFileReader<Product> {

    private final boolean compact;

    public ProductTitleFileReader(final String filename, final boolean compact) throws FileNotFoundException, IOException {
        super(filename);
        this.compact = compact;
    }

    @Override
    public Product next() {
        if (this.hasNext()) {
            final String[] parts = this.nextLine.split(" ", 2);
            final Product product = new Product();
            product.ASIN = parts[0];

            if (!compact) {
                product.title = parts.length == 2 ? parts[1] : null;
            }

            this.hasRead = false;
            return product;
        }
        else {
            throw new RuntimeException("Cannot call next() on empty iterator.");
        }
    }
}
