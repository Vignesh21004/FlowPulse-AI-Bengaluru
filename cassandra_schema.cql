-- FlowPulse AI — Apache Cassandra Distributed Time-Series Schema
CREATE KEYSPACE IF NOT EXISTS bengaluru_traffic
WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3};

USE bengaluru_traffic;

CREATE TABLE IF NOT EXISTS hub_realtime_metrics (
    zone text,
    hub_code text,
    event_date date,
    window_end timestamp,
    junction text,
    avg_speed double,
    pred_speed_30m double,
    avg_density double,
    rainfall_mm_hr double,
    congestion_index int,
    PRIMARY KEY ((zone, hub_code, event_date), window_end)
) WITH CLUSTERING ORDER BY (window_end DESC)
  AND default_time_to_live = 2592000;
