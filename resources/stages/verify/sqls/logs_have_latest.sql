select if(
(
select count(*) from `{{params.destination_dataset_project_id}}.{{params.dataset_name}}.logs`
where block_timestamp >= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:00:00.0") }}' and block_timestamp <= '{{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:59:59.999999") }}'
) > 0, 1,
cast((select 'There are no logs on {{ macros.ds_format(ts, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H") }}') as int64))
