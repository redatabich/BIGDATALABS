package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {
    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Usage: ReadHDFS <chemin_fichier_HDFS>");
            System.exit(1);
        }

        String hdfsFile = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path(hdfsFile);

        if (!fs.exists(path)) {
            System.out.println("Le fichier n'existe pas : " + hdfsFile);
            System.exit(1);
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }

        br.close();
        fs.close();
    }
}

