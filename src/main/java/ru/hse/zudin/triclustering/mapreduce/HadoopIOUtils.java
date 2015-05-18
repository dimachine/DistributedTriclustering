package ru.hse.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Sergey Zudin
 * @since 16.05.15.
 */
public class HadoopIOUtils {
    private static ObjectMapper mapper = new ObjectMapper();


    public static <T> T parseText(Text text, Class<T> cls, boolean decoded) throws IOException {
        String[] strings = text.toString().split("\t", 2);
        String content = strings.length > 1 ? strings[1] : strings[0];
        if (decoded) {
            //byte[] bytes = content.getBytes();
            byte[] bytes = Base64.getDecoder().decode(content);
            content = decompress(bytes);
        }
        return mapper.readValue(content, cls);
    }

    public static <T> Text asText(T tuple, boolean decoded) throws IOException {
        String string = mapper.writeValueAsString(tuple);
        if (decoded) {
            byte[] compress = compress(string);
            string = Base64.getEncoder().encodeToString(compress); //new String(compress, "UTF-8");
        }
        return new Text(string);
    }

    public static byte[] compress(String string) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream(string.length());
        GZIPOutputStream gos = new GZIPOutputStream(os);
        gos.write(string.getBytes());
        gos.close();
        byte[] compressed = os.toByteArray();
        os.close();
        return compressed;
    }

    public static String decompress(byte[] compressed) throws IOException {
        final int BUFFER_SIZE = 32;
        ByteArrayInputStream is = new ByteArrayInputStream(compressed);
        GZIPInputStream gis = new GZIPInputStream(is, BUFFER_SIZE);
        StringBuilder string = new StringBuilder();
        byte[] data = new byte[BUFFER_SIZE];
        int bytesRead;
        while ((bytesRead = gis.read(data)) != -1) {
            string.append(new String(data, 0, bytesRead));
        }
        gis.close();
        is.close();
        return string.toString();
    }

}
