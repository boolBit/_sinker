package com.duitang.sinker;

import java.io.File;
import java.io.FileOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Lz4Codec;

/**
 * 
 * @author kevx
 * @since 2:43:16 PM Jun 1, 2015
 */
public class Compressor {

    public static void main(String[] args) throws Exception {
        String fin = args[0];
        String fout = args[1];
        Lz4Codec codec = new Lz4Codec();
        org.apache.hadoop.io.compress.Compressor c = codec.createCompressor();
        CompressionOutputStream cos = 
                codec.createOutputStream(new FileOutputStream(new File(fout)));
        cos.write(FileUtils.readFileToByteArray(new File(fin)));
        cos.close();
        c.end();
    }

}
