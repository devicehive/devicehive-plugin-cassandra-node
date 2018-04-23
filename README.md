# devicehive-plugin-cassandra-node
DeviceHive Cassandra storage plugin written in Node.js
# Overview
This plugin allows you to store commands and notifications obtained through the DeviceHive platform in Cassandra. This application consists of 2 parts: the schema creation service and the plugin defined in the docker-compose file. 
<br /><br />
Upon starting the service the schema creation process will run first [to create a table and UDT schemas from JSON](#creating-tables-and-udts) and the plugin will check the state of the schema creation process using a predefined interval and number of checks. **The schema creation service always must only work on one node.** This was done to prevent concurrent schema modification that causes exceptions in Cassandra. But feel free to scale the plugin service as much as you need.
<br /><br />
At the very beginning of runtime both the plugin and schema creation service will [compare the existent table and UDT schemas with what is defined in JSON](#schema-comparison). In case of a column/field mismatch, column/field type mismatch, primary and clustering keys mismatch, or ordering mismatch, the application will fail. In case a UDT or table already exists it will notify the user. 
<br /><br />
When the message arrives it can be either a command, command update, or notification,.depending on what type it is data will be inserted into the appropriate [group of tables](#table-groups). Also the data will be filtered to [match the described table schema](#data-model).

# How it works

 1. Start DeviceHive
 2. Start Cassandra
 3. Create Cassandra keyspace
 4. Create following .env file. **Replace username, password, plugin topic, localhost, keyspace name, cassandra username and password with your values.**

    ENVSEPARATOR=_ <br />
    plugin_user_login=username<br />
    plugin_user_password=password <br />
    plugin_plugin_topic=plugin topic <br />
    plugin_device_hive_plugin_ws_endpoint=ws://localhost:3001 <br />
    plugin_device_hive_auth_service_api_url=http://localhost:8090/dh/rest <br />
    plugin_subscription_group=cassandra_plugin <br />
    cassandra_connection_keyspace=keyspace name <br />
    cassandra_connection_username=cassandra username <br />
    cassandra_connection_password=cassandra password <br />
    cassandra_connection_contactPoints=localhost

 5. Run `docker-compose up` (optionally add `--scale plugin=N`)
 6. Issue notification through DeviceHive
 7. Observe data in *notifications\_by\_deviceid* and *notifications\_by\_timestamp* tables:

<br />

	 notification | deviceid
	--------------+--------------------------------------
	         test | e50d6085-2aba-48e9-b1c3-73c673e414be


<br />

	 notification | timestamp                       | parameters
	--------------+---------------------------------+-------------------------------------------
	         test | 2018-02-27 17:17:27.963000+0000 | { "yourParam": "custom parameter value" }

# Configuration
## Plugin
Plugin part of configuration you can find [here](https://github.com/devicehive/devicehive-plugin-core-node#configuration).
## Cassandra
Cassandra part of plugin based on [DataStax Node.js driver](https://github.com/datastax/nodejs-driver) so please see [ClientOptions for DataStax driver](http://docs.datastax.com/en/developer/nodejs-driver/3.3/api/type.ClientOptions/) to get description of configs you are able to apply in this plugin.
There are two ways of configuring Cassandra:

 1. Share `cassandraConfig` directory as volume with docker container
 2. Redefine properties using `.env`

Configuration files are stored under `cassandraConfig` directory, there you can find two files: one is for actually Cassandra configuration (`config.json`) and another one for policies (`policies.json`). **Note: all configs described in environment variable format**
<br />
Example of `config.json`:

    {
      "connection": {
        "contactPoints": "127.0.0.1",
        "keyspace": "mykeyspace",
        "protocolOptions": {
          "port": "9042"
        },
        "username": "cassandra",
        "password": "cassandra"
      },

      "custom": {
        "schemaChecksCount": 5,
        "schemaChecksInterval": 1000
      }
    }
Cassandra config contains CUSTOM property for options not related to Cassandra connection. It can contain:

 - `schemaChecksCount` — how many times plugin service will check schema creation state on startup
 - `schemaChecksInterval` — how often (in milliseconds) plugin service will check schema creation state on startup

<br /><br />
Example of `policies.json`:

    "loadBalancing": {
	    "type": "WhiteListPolicy",
        "params": {
	        "childPolicy": {
                "type": "DCAwareRoundRobinPolicy",
                "params": {
	                "localDc": "127.0.0.1",
                    "usedHostsPerRemoteDc": 2
                }
            }
        }
    },
    "retry": {
        "type": "IdempotenceAwareRetryPolicy",
        "params": {
            "childPolicy": {
                "type": "RetryPolicy"
            }
        }
    }
All supported policies described [here](https://docs.datastax.com/en/developer/nodejs-driver/3.3/api/module.policies/).
<br />
How to compose policies:

 - **Class name** of specific policy is `type` property
 - **Arguments for policy constructor** described under `params` property
 - **Wrapping policies** (i.e. WhiteListPolicy of loadBalancing, IdempotenceAwareRetryPolicy of retry policy) must have `params` property with `childPolicy` that describes child policy in the same way (using `type` for class name and `params` for constructor arguments)

As was mentioned above you can redefine config options with .env file. In this case nesting of JSON is replaced with ENVSEPARATOR value and cassandra prefix with scope (connection or custom) should be added. Example:

    ENVSEPARATOR=_
    DEBUG=cassandrastoragemodule
    cassandra_connection_contactPoints=127.0.0.1, 192.168.1.1
    cassandra_connection_protocolOptions_port=9042
    cassandra_connection_policies_retry_type=RetryPolicy
    cassandra_custom_schema_checks_count=20
    cassandra_custom_schema_checks_interval=500

**Note: plugin supports all config options related to Cassandra connection taken from [here](http://docs.datastax.com/en/developer/nodejs-driver/3.3/api/type.ClientOptions/)**
If you want to have Cassandra module logging enabled put _DEBUG=cassandrastoragemodule_ in your environment variables.
# Tables and user defined types
This plugin allows you to configure tables and UDTs in JSON format which are to be created on application startup using schema creation service.
## Creating tables and UDTs
Schemas have to be described in **cassandraSchemas/cassandra-tables.json** and **cassandraSchemas/cassandra-user-types.json**. Feel free to modify these files and share volume with docker container (this is already done in docker-compose.yml).
Example of table schema description:

	{
	   "tables": {
	      "commands": {
             "command": "text",
             "deviceId": "text",
             "timestamp": "timestamp",
             "id": "int",
             "parameters": "text",

             "__primaryKey__": [ "command", "deviceId" ],
             "__clusteringKey__": [ "timestamp" ],
             "__order__": {
                "timestamp": "DESC"
             },
             "__options__": {
                "bloom_filter_fp_chance": 0.05,
                "compression": {
                   "chunk_length_in_kb": 128,
                   "class": "org.apache.cassandra.io.compress.SnappyCompressor"
                },
             },
             "__dropIfExists__": true
          },
       }
	}
Keys are column names and values are Cassandra data types. Also JSON schema has several reserved properties:

 - `__primaryKey__` — Array of column names which will represent primary key
 - `__clusteringKey__` — Array of column names which will represent clustering key
 - `__order__` — Object of order definition for clustering keys, values allowed:
         <br />
		 - DESC — for descending order
		 <br />
		 - ASC — for ascending order (default)

 - `__options__` — Object of any table options allowed by Cassandra (see [Cassandra table options](https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlCreateTable.html#ariaid-title4))
 - `__dropIfExists__` — Boolean value (false by default) indicates whether table or UDT should be dropped before creation

JSON schema for UDT is simpler, it contains fields with their types and `__dropIfExists__` only if necessary.
Example of UDT:

	{
	   "params": {
	      "name": "text",
	      "id": "int",
	      "address": "inet"
       }
	}
## Data model and table groups
### Data model
Table column definitions should stick to DeviceHive command and notification data models:

 - [Command representation](https://docs.devicehive.com/v3.4.3/docs/devicecommand#section-resource-representation)
 - [Notification representation](https://docs.devicehive.com/v3.4.3/docs/devicenotification#section-resource-representation)

This means you should define columns with same names as entity has to successfully save incoming data (but this doesn't mean you are not allowed to define any other columns).
<br />
Each incoming message is filtered by defined schema to extract properties that are described in table/UDT schema. **Note: filtering is applicable for columns with UDT (see parameters below).** So you can have multiple tables with different partition keys, ordering and columns set to store one entity, for example:
<br />
*we will store only command name with device ID in one table and command name with timestamp in another*

	    {
	       "command_by_deviceid": {
	          "command": "text",
	          "deviceId": "text",
	          "__primaryKey__": [ "command" ],
	          "__clusteringKey__": [ "deviceId" ]
	       },
	       "command_by_timestamp": {
	          "command": "text",
	          "timestamp": "timestamp",
	          "__primaryKey__": [ "command" ],
	          "__clusteringKey__": [ "timestamp" ]
	       },
	    }
If you have defined some column that doesn't exist in message `null` will be inserted as value for such column in Cassandra.
#### Parameters field
Command and notification data may contain parameters field which is arbitrary JSON value. For parameters column you can use following types:

 - text (varchar) or ascii — parameters value will be saved as JSON string
 - map<text, text> or map<ascii, ascii>, map<ascii, text> etc. — parameters value will be inserted as map with appropriate keys and values
 - UDT — parameters value will be filtered by some UDT defined separately

Also `frozen` use is allowed for *UDT* or *map* parameters.
<br />
Example of UDT parameters:
<br />
*UDT schema*

    {
	   "params": {
	      "username": "text",
	      "age": "int"
	   }
    }
*Table schema*

    {
       "command": "text",
       "deviceId": "text",
       "parameters": "frozen<params>",
       "__primaryKey__": [ "command" ],
       "__clusteringKey__": [ "deviceId" ]
    }
Then you can issue a command through DeviceHive and only described data model in schemas will be stored:

    {
       "command": "my-command",
       "deviceId": "my-device",
       "thisFieldWillNotBeStored": true,
       "parameters": {
	      "username": "John Doe",
	      "age": "30",
	      "thisFieldWillNotBeStoredAlso": true
	   }
    }

### Table groups
According to DeviceHive when the message comes in it can be one of two types: command or notification. Command message also can be command update. You can store any of these in arbitrary amount of tables just by defining table schemas and assigning tables to specific table group.
There are three table groups:
 - **command**
 - **notification**
 - **command updates** (store command updates separately, may be empty)

**Note: in case of empty `commandUpdatesTables` each command update will actually update existing command in all tables assigned to `commandTables`.**

You can assign any table to any group as well you can assign one table to multiple groups (many to many). Table groups are configured in **cassandraSchemas/cassandra-tables.json** with **commandTables**, **notificationTables**, **commandUpdatesTables** properties.
For example if you have:
<br />
`"notificationTables": [ "notifications_by_name", "notifications_by_deviceid", "notifications_by_X" ]`
<br />
each incoming notification will be inserted in all of these tables with structure that you have defined.
<br />
Or you may want to have shared table (*messages* in this example):

    "commandTables": [ "commands", "messages" ],
    "notificationTables": [ "notifications", "messages" ],
    "commandUpdatesTables": [ "command_updates", "messages" ]

## Schema comparison
When schema creation and plugin start up it compares tables and UDTs described in JSON schema with existent ones (if there are some). In case of inconsistency in columns/fields set or column/field types, primary keys, clustering keys or ordering applications will fail with appropriate messages. You can drop table or UDT before creation by [specifying `__dropIfExists__` as true](#creating-tables-and-udts) to prevent this kind of errors. **It's definitely not recommended to use this kind of schema recreation if you have some data in your tables.** If some table or UDT already exists application will just notify user about existent structure.
# Data path example
Lets take a look at how data is processed in Cassandra plugin and compare input data in plugin and output data in Cassandra.
<br />
Assume our command message looks like this:

    {
       "command": "my-command",
       "deviceId": "my-device",
       "timestamp": "2018-02-22T11:53:47.082Z",
       "parameters": {
          "name": "John Doe",
          "age": 30,
          "ip": "127.0.0.1"
       },
       "status": "some-status"
    }

And our JSON schemas of command tables are:
<br />
*commands_by_timestamp*

    {
       "command": "text",
       "timestamp": "timestamp",
       "parameters": "frozen<params>",
       "__primaryKey__": [ "command" ],
       "__clusteringKey__": [ "timestamp" ],
       "__order__": {
	      "timestamp": "DESC"
       }
    }
*commands_by_deviceid*

    {
       "command": "text",
       "deviceId": "text",
       "__primaryKey__": [ "command" ],
       "__clusteringKey__": [ "deviceId" ]
    }
Since *commands_by_timestamp* has `parameters` of UDT we will define `params` custom type:
<br />
*params*

    {
       "name": "text",
       "ip": "inet"
    }
After our message will be issued Cassandra plugin will catch that and insert into two tables with defined schema:
<br />
*commands_by_timestamp*

     command | timestamp                       | parameters
    ---------+---------------------------------+-------------------------------------
        test | 2018-02-22 11:53:47.082000+0000 | {name: 'John Doe', ip: '127.0.0.1'}


*commands_by_deviceid*

     command | deviceid
    ---------+--------------------------------------
        test | e50d6085-2aba-48e9-b1c3-73c673e414be

