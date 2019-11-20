package parser;

import com.mongodb.client.MongoCollection;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import org.bson.BsonDocument;
import org.bson.Document;

public class UpdateConsumer implements Runnable {

    private BlockingQueue<BsonDocument> updateQueue;

    private static MongoCollection<BsonDocument> data, history;

    static {
        data = MongoManager.getInstance()
                .getClient()
                .getDatabase("main")
                .getCollection("data", BsonDocument.class);

         history = MongoManager.getInstance()
                .getClient()
                .getDatabase("main")
                .getCollection("history", BsonDocument.class);
    }

    public UpdateConsumer(BlockingQueue<BsonDocument> queue) {
        this.updateQueue = queue;
    }

    public void run() {
        try {
            while (true) {
                BsonDocument entry = updateQueue.take();
                this.generateDiff(entry);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static ArrayList<BsonDocument> buffer = new ArrayList<>();
    private static long lastUpdateTime = System.currentTimeMillis();

    private void generateDiff(BsonDocument old) {
        Document filter = new Document();
        filter.append("_id", old.getObjectId("_id"));
        BsonDocument current = data.find(filter).first();
        if (Objects.equals(old, current)) {
            return;
        }

        BsonDocument historyDocument = Parser.parse(old, current);
        historyDocument.append("_ref", current.getObjectId("_id"));

        synchronized (UpdateConsumer.class) {
            buffer.add(historyDocument);

            if (buffer.size() >= 2000 || System.currentTimeMillis() - lastUpdateTime > 60_000) {
                history.insertMany(buffer);
                buffer.clear();
                lastUpdateTime = System.currentTimeMillis();
            }
        }
    }
}
