SELECT col7 , col0 , LAG(col0) OVER(PARTITION BY col7 ORDER BY col0) LAG_col0 FROM "allTypsUniq.parquet"