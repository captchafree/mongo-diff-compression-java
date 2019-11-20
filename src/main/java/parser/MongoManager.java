package parser;

import com.mongodb.MongoClient;

public class MongoManager {

    private static MongoManager instance;

    private MongoClient client;

    private MongoManager() {
        this.client = new MongoClient();
    }

    public static MongoManager getInstance() {
        if (instance == null) {
            instance = new MongoManager();
        }

        return instance;
    }

    public MongoClient getClient() {
        return this.client;
    }
}
