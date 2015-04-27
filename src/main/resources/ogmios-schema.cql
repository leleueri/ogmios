CREATE KEYSPACE IF NOT EXISTS ogmios WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE ogmios;

CREATE TABLE IF NOT EXISTS providers (
id varchar PRIMARY KEY,
name varchar,
description varchar,
registration timestamp,
eventTypes set<varchar>
);

CREATE TABLE IF NOT EXISTS eventtypes (
id varchar,
provid varchar,
description varchar,
registration timestamp,
type varchar,
unit varchar,
PRIMARY KEY (provid, id)
);

CREATE TABLE IF NOT EXISTS events (
id timeuuid,
provid varchar,
evttype varchar,
registration timestamp,
value varchar,
PRIMARY KEY (provid, evttype, id)
) WITH default_time_to_live = 16070400;
-- TTL set to around 6 months