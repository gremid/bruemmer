package com.middell.bruemmer;

import au.com.bytecode.opencsv.CSVReader;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.shared.DoesNotExistException;
import rx.Observable;
import rx.concurrency.Schedulers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.CollationKey;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author <a href="http://gregor.middell.net/" title="Homepage">Gregor Middell</a>
 */
@SuppressWarnings("unchecked")
public class PersonRecords {

    public static Observable<Map<String, Object>> create() {
        final Observable<List<String>> records = Observable.create(observer -> Schedulers.threadPoolForIO().schedule(() -> {
            try (CSVReader csv = new CSVReader(new InputStreamReader(new FileInputStream(RECORDS_FILE), CHARSET))) {
                int line = 0;
                while (true) {
                    final String[] record = csv.readNext();
                    if (record == null) {
                        break;
                    }
                    if (line++ == 0) {
                        continue;
                    }
                    observer.onNext(Arrays.asList(record));
                }
            } catch (IOException e) {
                observer.onError(e);
                return;
            }
            try (CSVReader csv = new CSVReader(new InputStreamReader(new FileInputStream(RECORDS_ADDENDUM_FILE), CHARSET))) {
                int line = 0;
                while (true) {
                    final String[] record = csv.readNext();
                    if (record == null) {
                        break;
                    }
                    if (line++ == 0) {
                        continue;
                    }
                    final ArrayList<String> fields = new ArrayList<>(Arrays.asList(record));
                    fields.remove(3);
                    observer.onNext(Collections.unmodifiableList(fields));
                }
            } catch (IOException e) {
                observer.onError(e);
                return;
            }
            observer.onCompleted();
        }));

        return records.parallel(shard -> shard.map(PersonRecords::parse).map(PersonRecords::gnd));
    }

    static Map<String, Object> parse(List<String> record) {
        final Map<String, Object> parsed = new HashMap<>();

        try {
            parsed.put("page", Integer.parseInt(record.get(0).trim()));
        } catch (NumberFormatException e) {
            // ignore
        }

        final String statusDesc = record.get(1).trim().toLowerCase();
        final SortedSet<String> status = new TreeSet<>();
        if (statusDesc.contains("*")) {
            status.add("present-in-remains");
        }
        if (statusDesc.contains("s.d.")) {
            status.add("alias");
        }
        parsed.put("status", status);


        String name = record.get(3).trim();
        final int nameTrailIndex = name.indexOf(";");
        name = nameTrailIndex < 0 ? name : name.substring(0, nameTrailIndex);
        parsed.put("name", name);
        parsed.put("collationKey", GERMAN_COLLATOR.getCollationKey(name));

        if (record.size() >= 5) {
            final String[] liveData = LIVE_DATA_SPLITTER.split(record.get(4));
            if (liveData.length == 2) {
                try {
                    parsed.put("born", Integer.parseInt(liveData[0].trim()));
                } catch (NumberFormatException e) {
                    // ignore
                }
                try {
                    parsed.put("died", Integer.parseInt(liveData[1].trim()));
                } catch (NumberFormatException e) {
                    // ignore
                }
            }
        }

        parsed.put("gnd", Collections.unmodifiableList(GND_REF_SPLITTER.splitAsStream(record.size() >= 6 ? record.get(5) : "")
                .map(String::trim)
                .map(ref -> GND_REF_INVALID_CHARS.matcher(ref).replaceAll(""))
                .filter(ref -> !ref.isEmpty())
                .filter(ref -> !ref.equalsIgnoreCase("no"))
                .sorted()
                .collect(Collectors.toList())));

        return parsed;
    }

    private static Model gndModel(String gnd) {
        final Model gndModel = ModelFactory.createDefaultModel();
        final String uri = "http://d-nb.info/gnd/" + gnd;
        final File cacheFile = new File(GND_CACHE_DIR, gnd + ".xml");
        if (cacheFile.lastModified() >= (System.currentTimeMillis() - (7 * 24 * 60 * 60000))) {
            try (InputStream gndData = new FileInputStream(cacheFile)) {
                gndModel.read(gndData, uri);
            } catch (IOException e) {
                // ignored
            }
        } else {
            try {
                gndModel.read(uri);
                try (OutputStream gndData = new FileOutputStream(cacheFile)) {
                    gndModel.write(gndData);
                } catch (IOException e) {
                    // ignored
                }
            } catch (DoesNotExistException e) {
                // ignored
            }
        }
        return gndModel;
    }

    private static Map<String, Object> gnd(Map<String, Object> record) {
        final SortedSet<String> professions = new TreeSet<>();
        Boolean male = null;
        Integer born = (Integer) record.get("born");
        Integer died = (Integer) record.get("died");
        for (String gnd : (List<String>) record.get("gnd")) {
            final Model gndModel = gndModel(gnd);
            for (final NodeIterator it = gndModel.listObjectsOfProperty(GND_PROFESSION); it.hasNext(); ) {
                final Statement preferredName = it.next().asResource().getProperty(GND_PREFERRED_NAME);
                if (preferredName != null) {
                    professions.add(GND_PROFESSION_GENDER_SUFFIX.matcher(preferredName.getString()).replaceAll(""));
                }
            }

            for (final NodeIterator it = gndModel.listObjectsOfProperty(GND_GENDER); it.hasNext(); ) {
                final Resource gender = it.next().asResource();
                if (male == null) {
                    if (gender.equals(GND_GENDER_MALE)) {
                        male = true;
                    } else if (gender.equals(GND_GENDER_FEMALE)) {
                        male = false;
                    }
                }
            }

            for (final NodeIterator it = gndModel.listObjectsOfProperty(GND_DATE_OF_BIRTH); it.hasNext(); ) {
                final Matcher matcher = GND_DATE_LEADING_YEAR.matcher(it.next().asLiteral().getString());
                if (matcher.matches()) {
                    born = Math.min(Integer.parseInt(matcher.group()), born == null ? Integer.MAX_VALUE : born);
                }
            }
            for (final NodeIterator it = gndModel.listObjectsOfProperty(GND_DATE_OF_DEATH); it.hasNext(); ) {
                final Matcher matcher = GND_DATE_LEADING_YEAR.matcher(it.next().asLiteral().getString());
                if (matcher.matches()) {
                    died = Math.max(Integer.parseInt(matcher.group()), died == null ? Integer.MIN_VALUE : died);
                }
            }
        }
        if (!professions.isEmpty()) {
            record.put("professions", professions);
        }
        if (born != null || died != null) {
            if (born != null) {
                record.put("born", born);
            }
            if (died != null) {
                record.put("died", died);
            }
            record.put("liveData", (born == null ? "" : born) + " - " + (died == null ? "" : died));
        }
        if (male != null) {
            SortedSet<String> status = (SortedSet<String>) record.get("status");
            status.add(male ? "male" : "female");
        }
        return record;
    }

    static final Collator GERMAN_COLLATOR = Collator.getInstance(Locale.GERMAN);
    static final Comparator<Map<String, Object>> GERMAN_COLLATION = (r1, r2) -> ((CollationKey) r1.get("collationKey")).compareTo((CollationKey) r2.get("collationKey"));

    static final File DATA_DIR = new File("data");
    static final File RECORDS_FILE = new File(DATA_DIR, "lexikon.csv");
    static final File RECORDS_ADDENDUM_FILE = new File(DATA_DIR, "lexikon-nachtrag.csv");
    static final File GND_CACHE_DIR = new File(DATA_DIR, "gnd");

    static final Charset CHARSET = Charset.forName("UTF-8");

    static final Pattern LIVE_DATA_SPLITTER = Pattern.compile("-");

    static final Pattern GND_REF_SPLITTER = Pattern.compile("[;,\\s]+");
    static final Pattern GND_REF_INVALID_CHARS = Pattern.compile("[^a-zA-Z0-9]");
    static final Pattern GND_PROFESSION_GENDER_SUFFIX = Pattern.compile("in$");
    static final Pattern GND_DATE_LEADING_YEAR = Pattern.compile("^[0-9]{4}");

    static final String GND_NS = "http://d-nb.info/standards/elementset/gnd#";
    static final String GND_GENDER_NS = "http://d-nb.info/standards/vocab/gnd/Gender#";

    private static Model GND_MODEL = ModelFactory.createDefaultModel();

    static final Property GND_PROFESSION = GND_MODEL.createProperty(GND_NS, "professionOrOccupation");
    static final Property GND_PREFERRED_NAME = GND_MODEL.createProperty(GND_NS, "preferredNameForTheSubjectHeading");

    static final Property GND_DATE_OF_BIRTH = GND_MODEL.createProperty(GND_NS, "dateOfBirth");
    static final Property GND_DATE_OF_DEATH = GND_MODEL.createProperty(GND_NS, "dateOfDeath");

    static final Property GND_GENDER = GND_MODEL.createProperty(GND_NS, "gender");
    static final Resource GND_GENDER_MALE = GND_MODEL.createResource(GND_GENDER_NS + "male");
    static final Resource GND_GENDER_FEMALE = GND_MODEL.createResource(GND_GENDER_NS + "female");

}
