-- Add version column to segments

ALTER TABLE segments ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
