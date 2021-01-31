package com.github.dlut;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.util.stream.Stream;

/**
 * HDFS I/O
 */
public class HdfsApplication {

    /**
     * Get the HDFS file system in case of later I/O
     * @return
     * @throws Exception
     */
    private FileSystem getFileSystem() throws Exception {
        String hdfsUri = "hdfs://ip:9820";
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsUri);
        configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsUri), configuration);
        return fileSystem;
    }

    /**
     * Read from HDFS and save the file in local
     * @param hdfsPath
     * @param localPath
     * @throws Exception
     */
    private void readHDFSFile(String hdfsPath, String localPath) throws Exception {
        FSDataInputStream fsDataInputStream = null;
        try {
            Path path = new Path(hdfsPath);
            fsDataInputStream = this.getFileSystem().open(path);

            BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream));
            System.out.println("begin reading from " + hdfsPath);
            Stream<String> data = reader.lines().parallel();

            BufferedWriter writer = new BufferedWriter(new FileWriter(localPath));
            System.out.println("begin writing to " + localPath);
            data.forEach(line -> {
                try {
                    writer.write(line);
                    writer.write("\n");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            reader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fsDataInputStream != null) {
                IOUtils.closeStream(fsDataInputStream);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        if (args.length < 3) {
            System.out.println("Usage: <read/write> <hdfsPath> <localPath>");
            return ;
        }
        HdfsApplication hdfsApp = new HdfsApplication();

        String mode = args[0];
        String hdfsPath = args[1];
        String localPath = args[2];

        if (mode.equals("read")) {
            long start = System.currentTimeMillis();
            hdfsApp.readHDFSFile(hdfsPath, localPath);
            long end = System.currentTimeMillis();
            System.out.println("read over, using time: " + (end - start) + "ms");
        } else {
            System.out.println("no support this mode now");
        }
    }
}
