\echo 'Executing RF2 deletes from dss.rd...'
\set ON_ERROR_STOP on

\if :{?refresh_rd}
\else
  \echo 'refresh_rd not provided; invoke psql with -v refresh_rd=/path/to/dss.rd' >&2
  \quit 1
\endif

CREATE TEMP TABLE rfdelete (o_orderkey bigint);

\copy rfdelete FROM :'refresh_rd' WITH (FORMAT csv);

DELETE FROM lineitem
WHERE l_orderkey IN (SELECT o_orderkey FROM rfdelete);

DELETE FROM orders
WHERE o_orderkey IN (SELECT o_orderkey FROM rfdelete);

DROP TABLE rfdelete;
