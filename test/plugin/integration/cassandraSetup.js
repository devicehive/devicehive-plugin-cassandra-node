module.exports = {
    dropTestKeyspace(cassandraDriverClient, keyspace) {
        return cassandraDriverClient.execute(`DROP KEYSPACE IF EXISTS ${keyspace}`);
    },

    async createTestKeyspace(cassandraDriverClient, keyspace) {
        await cassandraDriverClient.execute(`CREATE KEYSPACE ${keyspace} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }`);
        return cassandraDriverClient.execute(`USE ${keyspace}`);
    }
};