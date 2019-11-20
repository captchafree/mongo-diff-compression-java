package parser;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class Operations {

    private static final DocumentAdapter adapter = new DocumentAdapter(CodecRegistries.fromCodecs(new DocumentCodec()));

    private static MongoCollection<BsonDocument> data = MongoManager.getInstance()
            .getClient()
            .getDatabase("main")
            .getCollection("data", BsonDocument.class);

    private static BlockingQueue<BsonDocument> queue = new LinkedBlockingQueue<>(2500);

    static {
        new Thread(new UpdateConsumer(queue)).start();
    }

    public static Document updateOne(ObjectId id, Bson update, boolean upsert) {
        FindOneAndUpdateOptions ops = new FindOneAndUpdateOptions();
        ops.upsert(upsert);

        BsonDocument old = data.findOneAndUpdate(new Document().append("_id", id), update, ops);

        if (old != null) {
            try {
                queue.put(old);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return adapter.fromBson(old);
    }

    public static Document updateOne(ObjectId id, Bson update) {
        return updateOne(id, update, false);
    }

}
