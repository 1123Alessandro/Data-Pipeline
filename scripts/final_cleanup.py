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

preview = mm
preview.avail_date = pd.to_datetime(preview.avail_date) - pd.to_timedelta(7, unit='d')
view = pd.DataFrame(preview.groupby([pd.Grouper(key='avail_date', freq='W'), 'service']).price.sum())
view.to_excel('/mnt/c/Users/araza/Documents/1/git repos/Data-Pipeline/Weekly View.xlsx')
