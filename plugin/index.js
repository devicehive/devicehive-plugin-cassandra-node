const config = require(`./plugin-config`);
const { DeviceHivePlugin } = require(`devicehive-plugin-core`);
const CassandraPluginService = require('./CassandraPluginService');

DeviceHivePlugin.start(new CassandraPluginService(), config);
