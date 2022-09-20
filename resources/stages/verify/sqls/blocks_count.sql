select if(
(
select max(number) - min(number) from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`
where timestamp >= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:00:00.0") }}' and timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
) + 1 =
(
select count(*) from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.blocks`
where timestamp >= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:00:00.0") }}' and timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
), 1,
cast((select 'Total number of blocks except genesis is not equal to last block number {{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}') as int64))
