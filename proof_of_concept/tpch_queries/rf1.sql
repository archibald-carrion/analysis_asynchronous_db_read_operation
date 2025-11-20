\echo 'Loading RF1 refresh inserts from dss.ri...'
\set ON_ERROR_STOP on

\if :{?refresh_ri}
\else
  \echo 'refresh_ri not provided; invoke psql with -v refresh_ri=/path/to/dss.ri' >&2
  \quit 1
\endif

-- Orders rows have 9 columns; lineitems have 16 columns
\copy orders FROM PROGRAM 'awk -F"|" ''NF==9'' ":'refresh_ri'"' WITH (FORMAT csv, DELIMITER '|');
\copy lineitem FROM PROGRAM 'awk -F"|" ''NF==16'' ":'refresh_ri'"' WITH (FORMAT csv, DELIMITER '|');
