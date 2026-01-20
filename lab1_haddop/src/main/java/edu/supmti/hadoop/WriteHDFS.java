package edu.supmti.hadoop;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHDFS {
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: WriteHDFS <chemin_fichier_HDFS> <message>");
            System.exit(1);
        }

        String hdfsFile = args[0];
        String message = args[1];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(hdfsFile);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)));

        bw.write(message);
        bw.newLine();
        bw.close();
        fs.close();

        System.out.println("Message écrit dans : " + hdfsFile);
    }
}

