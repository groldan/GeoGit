/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.fs.mapped;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;

final class ByteData extends ByteArrayOutputStream {

    public ByteData(int initialBuffSize) {
        super(initialBuffSize);
    }

    public byte[] bytes() {
        return super.buf;
    }

    public int size() {
        return super.count;
    }

    public ByteData clear() {
        super.reset();
        return this;
    }

    public InputStream asInputStream() {
        return new ByteArrayInputStream(bytes());
    }

    public ByteBuffer asByteBuffer(int ensureSize) {
        reset();
        if (buf.length < ensureSize) {
            for (int i = 0; i < ensureSize - buf.length; i++) {
                super.write(0);
            }
        }
        return ByteBuffer.wrap(bytes());
    }
}