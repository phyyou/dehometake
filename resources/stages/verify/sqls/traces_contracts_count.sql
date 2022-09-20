select if(
(
select count(1)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces` as traces
where trace_type = 'create' and trace_address is null
    and date(block_timestamp) <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
) =
(
select count(*)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.transactions` as transactions
where receipt_contract_address is not null
    and block_timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
) and
(
select count(1)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces` as traces
where trace_type = 'create' and to_address is not null and status = 1
    and block_timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
) =
(
select count(*)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.contracts` as contracts
where block_timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
), 1,
cast((select 'Total number of traces with type create is not equal to number of contracts on {{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}') as int64))
