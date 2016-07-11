//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.google.common.io;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

@Beta
public final class Closeables {
    @VisibleForTesting
    static final Logger logger = Logger.getLogger(Closeables.class.getName());

    private Closeables() {
    }

    public static void close(@Nullable Closeable closeable, boolean swallowIOException) throws IOException {
        if(closeable != null) {
            try {
                closeable.close();
            } catch (IOException var3) {
                if(!swallowIOException) {
                    throw var3;
                }

                logger.log(Level.WARNING, "IOException thrown while closing Closeable.", var3);
            }

        }
    }

    public static void closeQuietly(@Nullable InputStream inputStream) {
        try {
            close(inputStream, true);
        } catch (IOException var2) {
            throw new AssertionError(var2);
        }
    }

    public static void closeQuietly(@Nullable Reader reader) {
        try {
            close(reader, true);
        } catch (IOException var2) {
            throw new AssertionError(var2);
        }
    }

    //HACK : backport this method for hbase (disable table)
    /**
     * Equivalent to calling {@code close(closeable, true)}, but with no
     * IOException in the signature.
     * @param closeable the {@code Closeable} object to be closed, or null, in
     *      which case this method does nothing
     */
    public static void closeQuietly(@Nullable Closeable closeable) {
        try {
            close(closeable, true);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "IOException should not have been thrown.", e);
        }
    }
}
