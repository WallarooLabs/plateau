-- Initial database provisioning

CREATE TABLE IF NOT EXISTS segments (
    id              INTEGER PRIMARY KEY,
    topic           STRING NOT NULL,
    partition       STRING NOT NULL,
    segment_index   INTEGER NOT NULL,
    time_start      DATETIME NOT NULL,
    time_end        DATETIME NOT NULL,
    record_start    INTEGER NOT NULL,
    record_end      INTEGER NOT NULL,
    size            INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS segments_segment_index ON segments(topic, partition, segment_index);

CREATE INDEX IF NOT EXISTS segments_start ON segments(topic, partition, record_start);

CREATE INDEX IF NOT EXISTS segments_time_range ON segments(topic, partition, time_start, time_end);
