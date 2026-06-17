\echo 'Loading RF1 refresh inserts (dss.ri.orders / dss.ri.lineitem)...'
\set ON_ERROR_STOP on

-- RF1 inserts new orders + lineitem rows. The refresh data is pre-split and
-- trailing-pipe-stripped by run_setup.sh into dss.ri.orders / dss.ri.lineitem,
-- which sit alongside this file in the scale's query dir. run_tests.sh runs psql
-- with its working directory set to that dir, so these relative \copy paths
-- resolve correctly. (psql's \copy cannot interpolate -v variables, which is why
-- the path is relative + the split is done at setup time rather than here.)
\copy orders   FROM 'dss.ri.orders'   WITH (FORMAT csv, DELIMITER '|');
\copy lineitem FROM 'dss.ri.lineitem' WITH (FORMAT csv, DELIMITER '|');
