select
  application_handle,
  client_idle_wait_time,
  total_rqst_time,
    total_wait_time,
    total_compile_proc_time,
    total_section_proc_time,
    total_commit_proc_time,
    total_rollback_proc_time,
    total_runstats_proc_time,
    total_reorg_proc_time,
    total_load_proc_time
from table(mon_get_unit_of_work(null,-1)) as x
;