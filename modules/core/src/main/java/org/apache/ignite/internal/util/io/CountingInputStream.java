/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.ignite.internal.util.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link InputStream} that counts the number of bytes read.
 *
 * @author Chris Nokleberg
 * @since 1.0
 */
public final class CountingInputStream extends FilterInputStream {

    private long count;
    private long mark = -1;

    /**
     * Wraps another input stream, counting the number of bytes read.
     *
     * @param in the input stream to be wrapped
     */
    public CountingInputStream(InputStream in) {
        super(in);
    }

    /** Returns the number of bytes read. */
    public long getCount() {
        return count;
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        int res = in.read();

        if (res != -1)
            count++;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b, int off, int len) throws IOException {
        int res = in.read(b, off, len);

        if (res != -1)
            count += res;

        return res;
    }

    /** {@inheritDoc} */
    @Override public long skip(long n) throws IOException {
        long res = in.skip(n);

        count += res;

        return res;
    }

    /** {@inheritDoc} */
    @Override public synchronized void mark(int readlimit) {
        in.mark(readlimit);

        mark = count;
        // it's okay to mark even if mark isn't supported, as reset won't work
    }

    /** {@inheritDoc} */
    @Override public synchronized void reset() throws IOException {
        if (!in.markSupported())
            throw new IOException("Mark not supported");

        if (mark == -1)
            throw new IOException("Mark not set");

        in.reset();

        count = mark;
    }
}