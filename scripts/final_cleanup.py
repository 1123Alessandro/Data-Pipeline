import pandas as pd

mm = pd.read_parquet('/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/ctbs_merged.parquet')

mm = mm[mm.avail_date < pd.Timestamp.today()]
mm = mm.drop(mm[mm.price == 0].index)
mm = mm.drop(mm[mm.branch_name.isna()].index)
mm = mm.drop(mm[mm.avail_date < mm.birthday].index)
mm = mm.drop_duplicates(['txn_id'])

mm.to_parquet('/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/Transaction Table.parquet')

print('Final Transaction Table:')
print(mm)
