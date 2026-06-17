\echo 'Executing RF2 deletes (dss.rd)...'
\set ON_ERROR_STOP on

-- RF2 deletes the orders (and their lineitems) named in dss.rd. The file holds one
-- order key per line and sits alongside this file in the scale's query dir;
-- run_tests.sh runs psql with its working directory set to that dir, so this
-- relative \copy path resolves correctly. (psql's \copy cannot interpolate -v
-- variables, hence the relative path.)
CREATE TEMP TABLE rfdelete (o_orderkey bigint);

\copy rfdelete FROM 'dss.rd' WITH (FORMAT csv);

DELETE FROM lineitem
WHERE l_orderkey IN (SELECT o_orderkey FROM rfdelete);

DELETE FROM orders
WHERE o_orderkey IN (SELECT o_orderkey FROM rfdelete);

DROP TABLE rfdelete;
