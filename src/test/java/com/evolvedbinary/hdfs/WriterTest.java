package com.evolvedbinary.hdfs;

import com.ctc.wstx.exc.WstxOutputException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

public class WriterTest {

    static final String FILE_PATH = "n:\\tmp\\sst\\data.sst";

    @BeforeAll
    public static void beforeAll() throws Exception {
        RocksDB.loadLibrary();
    }

    @Test
    public void test() throws Exception {
        FileUtils.deleteQuietly(new File(FILE_PATH));
        try (
                final var envOptions = new EnvOptions();
                final var options = new Options();
                final var writer = new SstFileWriter(envOptions, options)) {
            writer.open(FILE_PATH);
            ByteBuffer key = ByteBuffer.allocateDirect(8);
            ByteBuffer value = ByteBuffer.allocateDirect(256);
            Random r = new Random(1123123123123l);

            for (long l = 0 ; l < 1_000_000 ; l++) {
                key.clear().putLong(l).flip();

                value.clear().putLong(l).putLong(r.nextLong()).putLong(r.nextLong()).putLong(r.nextLong())
                        .putLong(l).putLong(r.nextLong()).putLong(r.nextLong()).putLong(r.nextLong())
                        .putLong(l).putLong(r.nextLong()).putLong(r.nextLong()).putLong(r.nextLong())
                        .putLong(l).putLong(r.nextLong()).putLong(r.nextLong()).putLong(r.nextLong())
                        .putLong(l).putLong(r.nextLong()).putLong(r.nextLong()).putLong(r.nextLong())
                        .putLong(l).putLong(r.nextLong()).putLong(r.nextLong()).putLong(r.nextLong())
                        .putLong(l).putLong(r.nextLong()).putLong(r.nextLong()).putLong(r.nextLong())
                        .putLong(l).putLong(r.nextLong()).putLong(r.nextLong()).putLong(r.nextLong())
                        .flip();
                writer.put(key, value);
                if(l % 500_000 == 0) {
                    System.out.println(l);
                }
            }
            writer.finish();
        }
    }


    @Test
    public void readSst() throws Exception {
        try (
                final var options = new Options();
                final var readOptions = new ReadOptions();
                final var reader = new SstFileReader(options);
        ) {
            reader.open(FILE_PATH);
            var iterator = reader.newIterator(readOptions);
            iterator.seekToFirst();

            ByteBuffer key = ByteBuffer.allocateDirect(8);
            ByteBuffer value = ByteBuffer.allocateDirect(256);

            while (iterator.isValid()) {
                key.clear();
                value.clear();
                iterator.key(key);
                iterator.value(value);
                //key.flip();
                var key_value = key.getLong();
                if(key_value % 500_000 == 0) {
                    System.out.println(key_value);
                }
                iterator.next();
            }
        }
    }

}
