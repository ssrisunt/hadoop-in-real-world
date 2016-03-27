package com.srisunt;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

/**
 * Hello world!
 */
public class HdfsWrite {
    public static void main(final String[] args) throws Exception {

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("vagrant");


        ugi.doAs(new PrivilegedExceptionAction<Void>() {

            public Void run() throws Exception {

                Configuration conf = new Configuration();
                //fs.default.name should match the corresponding value
                // in your core-site.xml in hadoop cluster
                conf.set("fs.default.name", "hdfs://vm-cluster-node1:8020");
                conf.set("hadoop.job.ugi", "vagrant");
                // in case you are running mapreduce job , need to set
                // 'mapred.job.tracker' as you did
                //conf.set("mapred.job.tracker", "hostname:port");


                String localSrc = args[0];
                String dst = args[1];
                //String uri = args[2];

                InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

                FileSystem fs = FileSystem.get(URI.create(dst), conf);
                OutputStream out = fs.create(new Path(dst));

                IOUtils.copyBytes(in, out, 4096, true);

                System.out.println("Connecting to ---" + conf.get("fs.defaultFS"));

                return null;
            }
        });

    }
}
