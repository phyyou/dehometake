select if(
(
select count(transaction_hash)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces`
where trace_address is null and transaction_hash is not null
    and block_timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
) =
(
select count(*)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions`
where block_timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
), 1,
cast((select 'Total number of traces with null address is not equal to transaction count on {{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}') as int64))
