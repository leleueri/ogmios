CREATE KEYSPACE IF NOT EXISTS ogmios WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

USE ogmios;

CREATE TABLE IF NOT EXISTS providers (
id varchar PRIMARY KEY,
name varchar,
description varchar,
registration timestamp,
eventTypes set<varchar>
);
