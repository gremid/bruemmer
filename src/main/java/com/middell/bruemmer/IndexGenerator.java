package com.middell.bruemmer;

import com.samskivert.mustache.Mustache;
import rx.Observable;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="http://gregor.middell.net/" title="Homepage">Gregor Middell</a>
 */
@SuppressWarnings("unchecked")
public class IndexGenerator {

    public static void main(String... args) {
        final long startTime = System.currentTimeMillis();
        final Map<String, Object> result = PersonRecords.create()
                .toSortedList(PersonRecords.GERMAN_COLLATION::compare)
                .flatMap(Observable::from)
                .reduce(new HashMap<>(), IndexGenerator::result)
                .toBlocking()
                .single();
        try (
                Reader templateReader = new InputStreamReader(IndexGenerator.class.getResourceAsStream("/index.mustache"));
                Writer resultWriter = new OutputStreamWriter(new FileOutputStream("htdocs/index.html"), Charset.forName("UTF-8"))
        ) {
            Mustache.compiler().compile(templateReader).execute(result, resultWriter);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Generated in " + (System.currentTimeMillis() - startTime) + " ms.");
    }

    private static Map<String, Object> result(Map<String, Object> result, Map<String, Object> record) {
        final List<Object> persons = (List<Object>) result.computeIfAbsent("persons", k -> new LinkedList<>());
        persons.add(record);
        return result;
    }
}
