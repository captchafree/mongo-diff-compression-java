package parser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;

/**
 * A naive strategy to compute the difference between two documents
 */
public class Parser {

    private static final int WORK_UNIT_SIZE = 8192;
    public static final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);

    private static BsonDocument diff;

    /**
     * Computes the diff between two bson documents
     * @param before The bson document before modification
     * @param after The bson document after modification
     * @return A document containing the difference between the provided documents
     */
    public static BsonDocument parse(BsonDocument before, BsonDocument after) {
        diff = new BsonDocument();
        computeDiffDict(before, after, diff);
        return diff;
    }

    /**
     * Computes the difference between two bson values
     */
    private static void computeDiff(BsonValue before, BsonValue after, BsonDocument diff, String key) {
        if (Objects.equals(before, after)) {
            return;
        }

        if (before instanceof BsonDocument && after instanceof BsonDocument) {
            if (!diff.containsKey(key)) {
                diff.put(key, new BsonDocument());
            }
            computeDiffDict(before.asDocument(), after.asDocument(), diff.getDocument(key));
        } else if (before instanceof BsonArray && after instanceof BsonArray) {
            if (!diff.containsKey(key)) {
                diff.put(key, new BsonDocument());
            }
            computeDiffList(before.asArray(), after.asArray(), diff.getDocument(key), key);
        } else {
            synchronized (diff) {
                diff.put(key, before);
            }
        }
    }

    /**
     * Computes the difference between two bson documents
     */
    private static void computeDiffDict(BsonDocument before, BsonDocument after, BsonDocument diff) {
        Set<String> keys = new HashSet<>();
        keys.addAll(before.keySet());
        keys.addAll(after.keySet());

        if (keys.size() >= WORK_UNIT_SIZE) {
            Iterator<String> itr = keys.iterator();
            List<Set<String>> partitions = new ArrayList<>();
            partitions.add(new HashSet<>());
            final CountDownLatch latch = new CountDownLatch((keys.size() / WORK_UNIT_SIZE));

            int iteration = 0;
            while (itr.hasNext()) {
                if (partitions.get(iteration).size() < WORK_UNIT_SIZE) {
                    partitions.get(iteration).add(itr.next());
                }

                if  (partitions.get(iteration).size() == WORK_UNIT_SIZE || !itr.hasNext()) {
                    int finalIteration = iteration;
                    executor.submit(() -> {
                        for (String key : partitions.get(finalIteration)) {
                            computeDiff(before.get(key), after.get(key), diff, key);
                        }
                        latch.countDown();
                    });
                    partitions.add(new HashSet<>());
                    iteration++;
                }
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return;
        }

        for (String key : keys) {
            BsonValue old = before.get(key);
            BsonValue curr = after.get(key);

            computeDiff(old, curr, diff, key);
        }
    }

    /**
     * Computes difference between two bson arrays
     */
    private static void computeDiffList(BsonArray before, BsonArray after, BsonDocument diff, String key) {
        int maxLength = Math.max(before.size(), after.size());
        BsonArray result = new BsonArray();

        for (int i = 0; i < maxLength; i++) {
            BsonValue old = i < before.size() ? before.get(i) : null;
            BsonValue curr = i < after.size() ? after.get(i) : null;

            if (Objects.equals(old, curr)) {
                continue;
            }

            BsonDocument elemDiff = new BsonDocument();
            computeDiff(old, curr, elemDiff, key);

            BsonDocument obj = new BsonDocument();
            obj.append("index", new BsonInt32(i));
            obj.append("diff", elemDiff.getOrDefault(key, elemDiff));

            result.add(obj);
        }

        diff.put(key, result);
    }

}
