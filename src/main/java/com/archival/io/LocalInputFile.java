package com.archival.io;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Counterpart to {@link LocalOutputFile}: lets Parquet's reader APIs
 * ({@code ParquetFileReader}, {@code AvroParquetReader}) read a plain local
 * file directly, without requiring a Hadoop {@code FileSystem}. Used by the
 * reconciliation step to read back and validate the files the export step
 * just wrote.
 */
public class LocalInputFile implements InputFile {

    private final Path path;

    public LocalInputFile(Path path) {
        this.path = path;
    }

    @Override
    public long getLength() throws IOException {
        return Files.size(path);
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new LocalSeekableInputStream(new RandomAccessFile(path.toFile(), "r"));
    }

    private static class LocalSeekableInputStream extends SeekableInputStream {
        private final RandomAccessFile raf;

        LocalSeekableInputStream(RandomAccessFile raf) {
            this.raf = raf;
        }

        @Override
        public long getPos() throws IOException {
            return raf.getFilePointer();
        }

        @Override
        public void seek(long newPos) throws IOException {
            raf.seek(newPos);
        }

        @Override
        public int read() throws IOException {
            return raf.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return raf.read(b, off, len);
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            raf.readFully(bytes);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            raf.readFully(bytes, start, len);
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            byte[] tmp = new byte[buf.remaining()];
            int n = raf.read(tmp);
            if (n > 0) {
                buf.put(tmp, 0, n);
            }
            return n;
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            byte[] tmp = new byte[buf.remaining()];
            raf.readFully(tmp);
            buf.put(tmp);
        }

        @Override
        public void close() throws IOException {
            raf.close();
        }
    }
}
