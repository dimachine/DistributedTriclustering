package ru.hse.zudin.triclustering.mapreduce;

import org.apache.hadoop.io.Text;
import org.codehaus.jackson.map.ObjectMapper;
import ru.hse.zudin.triclustering.model.Tuple;

import java.io.IOException;

/**
 * @author Sergey Zudin
 * @since 16.05.15.
 */
public class HadoopIOUtils {
    private static ObjectMapper mapper = new ObjectMapper();

    public static Tuple parseTuple(Text text) throws IOException {
        String[] strings = text.toString().split("\t");
        String content = strings.length > 1 ? strings[1] : strings[0];
        return mapper.readValue(content, Tuple.class);
    }

    public static Tuple[] parseTuples(Text text) throws IOException {
        String[] strings = text.toString().split("\t");
        String content = strings.length > 1 ? strings[1] : strings[0];
        return mapper.readValue(content, Tuple[].class);
    }

    public static Text asText(Tuple tuple) throws IOException {
        return new Text(mapper.writeValueAsString(tuple));
    }

    public static Text asText(Tuple[] tuples) throws IOException {
        return new Text(mapper.writeValueAsString(tuples));
    }
}
