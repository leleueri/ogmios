CREATE KEYSPACE IF NOT EXISTS ogmios WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE ogmios;
CREATE TABLE IF NOT EXISTS providers (
    id varchar PRIMARY KEY,
    name varchar,
    registration timestamp,
    ref map<varchar, varchar>
) WITH comment='Entity that generates metrics and events';
CREATE INDEX IF NOT EXISTS ON providers (name);
CREATE TABLE IF NOT EXISTS metrics (
    providerid varchar,
    metricname varchar,
	registration timestamp,
    generation timestamp,
    value double,
   	PRIMARY KEY ((providerid, metricname), generation)
) WITH comment='All Metrics grouped by ProviderId/MetricName. Registration is the date when the metric has been saved in cassandra, generation is the date at which the metric has been taken by the provider';
CREATE TABLE IF NOT EXISTS events (
    providerid varchar,
    eventname varchar,
	registration timestamp,
    generation timestamp,
    properties map<varchar, varchar>,
   	PRIMARY KEY ((providerid, eventname), generation)
) WITH comment='All event grouped by ProviderId/EventName. Registration is the date when the event has been saved in cassandra, generation is the date at which the event has been taken by the provider';