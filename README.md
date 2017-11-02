# Kafka Connect JDBC Connector

kafka-connect-jdbc is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data to and from any JDBC-compatible database.

Documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-jdbc/docs/index.html).

# Enhancements
Original project can be found [here](https://github.com/confluentinc/kafka-connect-jdbc).
The enhancements made in this fork are primarily for Postgres. They are as follows: 
## Data Anonymization 
Added support for anonymizing data at a column specific level for any database table. SHA-256 encryption is used for anonymization. Columns with the following data types are supported - Text, TextArray, Json. To anonymize a column add the following in the SourceConnector configuration. <br /> `<table-name>.anonymize.column.name = <column-name>`.

## Efficient Polling of tables
Schema's without an incrementing/timestamp column in one or more tables have to use the inefficient bulk mode for polling every table. This enhancement allows setting polling modes in the SourceConnector at a table specific level. This allows configuring of different polling modes for each table in the SourceConnector configuraion. <br />
`<table-name>.mode = incrementing` <br />
`<table-name>.incrementing.column.name = <column-name>`
 
## Data Deduplication
Built in support for data deduplication is already provided using the upsert mode. An update is performed instead of an insert in the Sink database using a record's Primary Key. However primary keys are not passed from Source to Sink. We pass Primary Keys from Source to Sink by creating a KeySchema for each record and use upsert mode for data deduplication. <br />
`insert.mode = upsert` <br />
`pk.fields = record_key`

# Development

To build a development version you'll need a recent version of Kafka. You can build
kafka-connect-jdbc with Maven using the standard lifecycle phases.


# Contribute

- Source Code: https://github.com/confluentinc/kafka-connect-jdbc
- Issue Tracker: https://github.com/confluentinc/kafka-connect-jdbc/issues


# License

The project is licensed under the Apache 2 license.
