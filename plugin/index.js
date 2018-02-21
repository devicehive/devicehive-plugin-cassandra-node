const config = require(`./plugin-config`);
const cassandraConfig = require(`./config`).cassandra;
const { DeviceHivePlugin } = require(`devicehive-plugin-core`);
const CassandraPluginService = require('./CassandraPluginService');

DeviceHivePlugin.start(new CassandraPluginService(cassandraConfig), config);
