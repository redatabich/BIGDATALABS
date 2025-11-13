package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) throws IOException {

        if (args.length < 3) {
            System.out.println("Usage: HadoopFileStatus <chemin> <nom_fichier> <nouveau_nom>");
            System.exit(1);
        }

        String dirPath = args[0];         // ex: /user/root/input
        String fileName = args[1];        // ex: purchases.txt
        String newFileName = args[2];     // ex: achats.txt

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filepath = new Path(dirPath, fileName);

        if (!fs.exists(filepath)) {
            System.out.println("File does not exist");
            System.exit(1);
        }

        FileStatus status = fs.getFileStatus(filepath);

        System.out.println("File Name: " + filepath.getName());
        System.out.println("File Size: " + status.getLen() + " bytes");
        System.out.println("File owner: " + status.getOwner());
        System.out.println("File permission: " + status.getPermission());
        System.out.println("File Replication: " + status.getReplication());
        System.out.println("File Block Size: " + status.getBlockSize());

        BlockLocation[] blockLocations =
                fs.getFileBlockLocations(status, 0, status.getLen());

        for (BlockLocation blockLocation : blockLocations) {
            String[] hosts = blockLocation.getHosts();
            System.out.println("Block offset: " + blockLocation.getOffset());
            System.out.println("Block length: " + blockLocation.getLength());
            System.out.print("Block hosts: ");
            for (String host : hosts) {
                System.out.print(host + " ");
            }
            System.out.println();
        }

        // Renommage
        Path newPath = new Path(dirPath, newFileName);
        boolean ok = fs.rename(filepath, newPath);
        System.out.println("Renamed: " + ok);

        fs.close();
    }
}

