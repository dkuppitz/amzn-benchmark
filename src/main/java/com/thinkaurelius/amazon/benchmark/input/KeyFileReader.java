/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.input;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public class KeyFileReader extends InputFileReader<String> {

    public KeyFileReader(final String filename) throws FileNotFoundException, IOException {
        super(filename);
    }

    @Override
    public String next() {
        if (this.hasNext()) {
            this.hasRead = false;
            return this.nextLine;
        }
        else {
            throw new RuntimeException("Cannot call next() on empty iterator.");
        }
    }
}
