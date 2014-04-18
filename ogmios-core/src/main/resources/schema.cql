CREATE KEYSPACE ogmios WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
CREATE TABLE providers (
    id varchar PRIMARY KEY,
    name varchar,
    registration timestamp,
    ref map<varchar, varchar>
) WITH comment='Entity that generates metrics and events'
   AND read_repair_chance = 1.0;
CREATE INDEX ON providers (name);
   