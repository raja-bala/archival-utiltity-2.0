package com.archival.io;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * A minimal {@link OutputFile} that writes straight to the local filesystem
 * via {@code java.nio.file}. Parquet's {@code parquet-hadoop} module normally
 * expects a Hadoop {@code FileSystem}, which drags in the full Hadoop
 * runtime; this class lets {@code AvroParquetWriter} target a plain local
 * path instead, keeping the archival utility's footprint small and its
 * behavior independent of any Hadoop/HDFS configuration.
 */
public class LocalOutputFile implements OutputFile {

    private static final int DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;

    private final Path path;

    public LocalOutputFile(Path path) {
        this.path = path;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        if (Files.exists(path)) {
            throw new IOException("File already exists: " + path);
        }
        Files.createDirectories(path.getParent());
        OutputStream out = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        return new LocalPositionOutputStream(out);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        Files.createDirectories(path.getParent());
        OutputStream out = Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        return new LocalPositionOutputStream(out);
    }

    @Override
    public boolean supportsBlockSize() {
        return true;
    }

    @Override
    public long defaultBlockSize() {
        return DEFAULT_BLOCK_SIZE;
    }

    private static class LocalPositionOutputStream extends PositionOutputStream {
        private final OutputStream delegate;
        private long pos = 0;

        LocalPositionOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            pos += 1;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            pos += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
