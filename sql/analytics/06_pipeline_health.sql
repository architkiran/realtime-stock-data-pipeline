-- Latest data-quality results.
SELECT run_date, "check", status, detail
FROM stockstream.dq_results
ORDER BY run_date DESC, "check";
