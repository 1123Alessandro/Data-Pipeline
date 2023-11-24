import pandas as pd

bs = pd.read_parquet('/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/bs_dates.parquet')
ct = pd.read_parquet('/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/ct_dates.parquet')

def conform(x):
    return None if x in ['', 'N/A'] else x


bs.branch_name = bs.branch_name.apply(conform)
bs.price = bs.price.mask(bs.price.isna(), 0)

mm = pd.merge(ct, bs)

mm.to_parquet('/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/ctbs_merged.parquet')
