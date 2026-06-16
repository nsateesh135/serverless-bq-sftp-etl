-- Example extraction query against a BigQuery public dataset, included so
-- the pipeline can be run end-to-end without access to any private data.
--
-- Every `.sql` file in this directory is picked up automatically by
-- `BigQueryClient.execute_query_and_store_in_tmp` (see main.py): it's
-- rendered as a Jinja2 template, run as a BigQuery query, and the result
-- is written to `/tmp/uploads/<filename>.csv`, then uploaded to both the
-- SFTP server and Cloud Storage. Drop in any number of additional
-- `<name>.sql` files here to extract more datasets - no code changes
-- required.
select
  word,
  word_count,
  corpus
from `bigquery-public-data.samples.shakespeare`
order by word_count desc
limit 100
