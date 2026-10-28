index email_delivered_ts: CREATE INDEX email_delivered_ts on email_delivered(ts)
index webpush_delivered_ts: CREATE INDEX webpush_delivered_ts on webpush_delivered(ts)
table email_delivered: CREATE TABLE email_delivered (address text not null, event_id text not null, ts integer not null, primary key (address, event_id))
table webpush_delivered: CREATE TABLE webpush_delivered (endpoint text not null, event_id text not null, ts integer not null, primary key (endpoint, event_id))
