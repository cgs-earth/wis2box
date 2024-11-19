-- init-uuid-ossp.sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- https://github.com/FraunhoferIOSB/FROST-Server/discussions/2047
ALTER TABLE public."OBSERVATIONS" DISABLE TRIGGER datastreams_actualization_insert; 
