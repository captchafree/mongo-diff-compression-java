import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.conversions.Bson;
import parser.DocumentAdapter;
import parser.Parser;

public class Operations {

    private static final DocumentAdapter adapter = new DocumentAdapter(CodecRegistries.fromCodecs(new DocumentCodec()));

    private static MongoCollection<BsonDocument> data = new MongoClient().getDatabase("main").getCollection("data", BsonDocument.class);
    private static MongoCollection<BsonDocument> history = new MongoClient().getDatabase("main").getCollection("history", BsonDocument.class);

    private static final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

    public static Document updateOne(Bson query, Bson update, boolean upsert) {
        FindOneAndUpdateOptions ops = new FindOneAndUpdateOptions();
        ops.upsert(upsert);

        BsonDocument old = data.findOneAndUpdate(query, update, ops);
        if (old != null) {
            generateDiff(old);
        }

        return adapter.fromBson(old);
    }



    private static ArrayBlockingQueue<BsonDocument> queue = new ArrayBlockingQueue<>(10_000);

    private static void generateDiff(BsonDocument old) {
        executor.submit(() -> {
            Document filter = new Document();
            filter.append("_id", old.getObjectId("_id"));
            BsonDocument current = data.find(filter).first();
            if (Objects.equals(old, current)) {
                return;
            }

            BsonDocument historyDocument = Parser.parse(old, current);
            historyDocument.remove("_id");

            historyDocument.append("_ref", current.getObjectId("_id"));

            boolean added;
            do {
                added = queue.offer(historyDocument);
            } while (!added);


            if(queue.size() >= 2000) {
                new Thread(() -> {
                    List<BsonDocument> data = new ArrayList<>();
                    queue.drainTo(data, 2000);
                    history.insertMany(data);
                }).start();
            }
        });
    }

    public static Document updateOne(Bson query, Bson update) {
        return updateOne(query, update, false);
    }

}
