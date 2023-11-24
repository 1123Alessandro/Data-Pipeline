import pandas as pd

bs = pd.read_parquet('/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/files/bs_duplicates_names.parquet')
ct = pd.read_parquet('/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/files/ct_duplicates_names.parquet')

ct.birthday = pd.to_datetime(ct.birthday)
ct.avail_date = pd.to_datetime(ct.avail_date)

bs.to_parquet('/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/files/bs_dates.parquet')
ct.to_parquet('/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/files/ct_dates.parquet')
