import pandas as pd

bs = pd.read_parquet('./bs_duplicates_names.parquet')
ct = pd.read_parquet('./ct_duplicates_names.parquet')

ct.birthday = pd.to_datetime(ct.birthday)
ct.avail_date = pd.to_datetime(ct.avail_date)

bs.to_parquet('bs_dates.parquet')
ct.to_parquet('ct_dates.parquet')
