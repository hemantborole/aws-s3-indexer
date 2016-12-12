select a.name, a.lines, a.bytes, a.loadtime, b.pct_complete
from stl_file_scan a, STV_LOAD_STATE b
where a.query = b.query
order by b.pct_complete desc;
