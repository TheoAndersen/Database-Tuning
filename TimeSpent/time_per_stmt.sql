select
  substr(stmt_text, 1, 40) stmt_text,
  num_executions,
  stmt_exec_time,
    total_act_wait_time,
    total_routine_non_sect_proc_time,
    total_section_proc_time
from table(mon_get_pkg_cache_stmt('d', null, null,-1)) as x
order by stmt_exec_time desc
fetch first 10 rows only
;