Delete from production_log; VACUUM;


select * from production_log where start > 20200000 and end < 20200404 and sid_id = 70;

select sid_id, count(*) from production_log group by sid_id;

Delete from production_log; VACUUM;