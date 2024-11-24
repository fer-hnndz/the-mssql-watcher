SELECT 
    Operation,
    Context,
    [Transaction ID],
    AllocUnitName,
    [RowLog Contents 0],
    [RowLog Contents 1],
    [RowLog Contents 2],
    [RowLog Contents 3],
    *
FROM fn_dblog(NULL, NULL)
WHERE AllocUnitName IS NOT NULL 
  AND AllocUnitName NOT LIKE 'sys.%'
  AND AllocUnitName NOT LIKE 'Unknown Alloc%';
