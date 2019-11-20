import java.util.Random;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.types.ObjectId;
import parser.Operations;
import parser.Parser;

public class Main {

    static String beforeJson = "{\n"
            + "    'a': {\n"
            + "        'b': {\n"
            + "            'c': {\n"
            + "                'd': 1\n"
            + "            }\n"
            + "        },\n"
            + "        'g': 2\n"
            + "    },\n"
            + "    'e': 3,\n"
            + "    'f': [4,5,6]\n"
            + "}";

    static String afterJson = "{\n"
            + "    'a': {\n"
            + "        'b': {\n"
            + "            'c': {\n"
            + "                'd': 10\n"
            + "            }\n"
            + "        },\n"
            + "        'g': 20\n"
            + "    },\n"
            + "    'e': {\n"
            + "        'z': [1]\n"
            + "    },\n"
            + "    'f': [4,6]\n"
            + "}";

    public static void main(String[] args) {
        runBenchmark();
        // System.out.println(Parser.parse(BsonDocument.parse(beforeJson), BsonDocument.parse(afterJson)));

        /*
        BsonDocument before = new BsonDocument();
        BsonDocument after = new BsonDocument();

        for (int j = 0; j < 1000; j++) {
            for (int i = 0; i < 50_000; i++) {
                before.put(i+"", new BsonInt32(i));
                after.put(i+"", new BsonInt32(i+1));
            }
        }
        long[] times = new long[1000];
        for (int i = 0; i < times.length; i++) {
            long start = System.currentTimeMillis();
            Parser.parse(before, after);
            long end = System.currentTimeMillis();
            times[i] = end-start;
        }

        //BsonDocument result = Parser.parse(before, after);
        //System.out.println(result.toJson());
        //System.out.println(result.keySet().size());

        int sum = 0;
        for (int i = 0; i < times.length; i++) {
            sum += times[i];
        }
        System.out.println(sum/times.length);
         */
    }

    private static void runBenchmark() {
        Document[] updates = new Document[1_000_000];
        System.out.println("Generating updates...");
        for (int i = 0; i < updates.length; i++) {
            updates[i] = new Document().append("$set", getRandomUpdate());
        }

        System.out.println("Applying updates...");
        long start = System.currentTimeMillis();
        long middle = start;
        for (int i = 0; i < updates.length; i++) {
            Operations.updateOne(new Document().append("_id", new BsonObjectId(new ObjectId("5dd42316d34e26759027f476"))), updates[i]);

            if (i % 2000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("2000 processed in " + (now-middle)/1000.0 + " seconds");
                middle = now;
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Run Time: " + (end-start)/1000.0  + " seconds");
    }

    static String[] keys = new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"};
    static Random random = new Random(System.currentTimeMillis());
    private static String getRandomKey() {
        return keys[random.nextInt(keys.length)];
    }

    private static Document getRandomUpdate() {
        int numOfUpdates = random.nextInt(6) + 1;
        Document result = new Document();
        for(int i = 0; i < numOfUpdates; i++) {
            String key = getRandomKey();
            while (result.containsKey(key)) {
                key = getRandomKey();
            }
            result.put(key, new BsonInt32(random.nextInt(10000)+1));
        }
        return result;
    }
}
