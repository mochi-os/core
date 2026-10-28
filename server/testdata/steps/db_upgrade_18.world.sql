index worlds_seen: CREATE INDEX worlds_seen on worlds( seen )
table worlds: CREATE TABLE worlds ( peer text not null, world text not null, name text not null, address text not null, version integer not null, services text not null, seen integer not null, primary key (peer, world) )
