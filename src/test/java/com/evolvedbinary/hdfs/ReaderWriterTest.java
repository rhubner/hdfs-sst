package com.evolvedbinary.hdfs;

import com.ctc.wstx.exc.WstxOutputException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.util.NativeCodeLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.HdfsEnv;
import org.rocksdb.Options;
import org.rocksdb.SstFileReader;

import static com.evolvedbinary.hdfs.WriterTest.FILE_PATH;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;

public class ReaderWriterTest {

    MiniDFSCluster dfsCluster;
    String hdfsUrl;

    @Test
    public void simpleTest() throws Exception {
        try (var options = new Options();
             final var env = new HdfsEnv(hdfsUrl);
             final var sstReader = new SstFileReader(options);
        ) {
            sstReader.open("hdfs://data.sst");
            sstReader.verifyChecksum();
        }
        assertTrue(true);
    }

    @BeforeEach
    public void setup()  throws Exception {

        var hdfsDir = new File("N:\\tmp\\hdfs\\");
        var hadoopHome = new File("N:\\tmp\\hdfs\\");
        FileUtils.deleteQuietly(hdfsDir);

        var config = new Configuration();
        config.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDir.getAbsolutePath());
        config.set("hadoop.home.dir", hadoopHome.getAbsolutePath());

        var miniClusterBuilder = new MiniDFSCluster.Builder(config);

        dfsCluster = miniClusterBuilder.build();

        hdfsUrl = "hdfs://localhost:" + dfsCluster.getNameNodePort() + "/";

        var hdfsPath = new Path("/data.sst");

        System.out.println("Copying data from local FS");
        dfsCluster.getFileSystem().copyFromLocalFile(new Path(FILE_PATH), hdfsPath);
        System.out.println("Data copied from local FS");

        System.out.println("HDFS address : " + hdfsUrl);
    }

    @AfterEach
    public void tearDown() throws Exception {
        dfsCluster.shutdown();
    }



}
