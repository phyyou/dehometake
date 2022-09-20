select if(
(
select sum(transaction_count)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`
where timestamp >= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:00:00.0") }}' and timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
) =
(
select count(*)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions`
where block_timestamp >= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:00:00.0") }}' and block_timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
), 1,
cast((select 'Total number of transactions is not equal to sum of transaction_count in blocks table') as int64))
