select if(
(
select count(distinct(block_number))
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.traces`
where trace_type = 'reward' and reward_type = 'block'
    and block_timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
) =
(
select count(*)
from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`
where timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
) - 1, 1,
cast((select 'Total number of unique blocks in traces is not equal to block count minus 1 on {{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}') as int64))
