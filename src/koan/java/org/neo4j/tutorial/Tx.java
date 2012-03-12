package org.neo4j.tutorial;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

public class Tx {

    public static Transactable with(GraphDatabaseService db) {
        return new Transactable(db);
    }

    public static class Transactable {

        private final GraphDatabaseService db;

        public Transactable(GraphDatabaseService db) {
            this.db = db;
        }

        public void doTx(Runnable r) {
            Transaction tx = db.beginTx();

            try {
                r.run();
                tx.success();
            } finally {
                tx.finish();
            }

        }
    }

}
