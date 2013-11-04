/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.thinkaurelius.amazon.benchmark.input;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author Daniel Kuppitz <daniel at thinkaurelius.com>
 */
public abstract class InputFileReader<T> implements Closeable, Iterable<T>, Iterator<T> {
    
    private static final Logger logger = Logger.getLogger(InputFileReader.class.getName());

    private final BufferedReader reader;
    protected boolean hasRead;
    protected String nextLine;
    
    public InputFileReader(final String filename) throws FileNotFoundException, IOException {

        InputStream inputStream = new FileInputStream(filename);
        
        if (filename.toLowerCase().endsWith(".gz")) {
            inputStream = new GZIPInputStream(inputStream);
        }

        reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
        hasRead = false;
    }
    
    protected void readNextLine() {
        try {
            nextLine = reader.readLine();
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
            nextLine = null;
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        if (!hasRead) {
            readNextLine();
            hasRead = true;
        }
        return nextLine != null;
    }

    @Override
    public abstract T next();

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove() is not supported.");
    }
}
