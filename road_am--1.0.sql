/* road_am/road_am--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION road_am" to load this file. \quit

CREATE FUNCTION road_am_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

-- Access methods
CREATE ACCESS METHOD road TYPE TABLE HANDLER road_am_handler;
COMMENT ON ACCESS METHOD road IS 'table AM for Read-Only Archived Data';
