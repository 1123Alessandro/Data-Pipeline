import numpy as np
import pandas as pd
import re

bs = pd.read_json('./branch_service_transaction_info.json')
ct = pd.read_json('./customer_transaction_info.json')

ct = ct.drop_duplicates()
bs = bs.drop_duplicates()

ct.last_name = ct.last_name.str.capitalize()
ct.first_name = ct.first_name.str.capitalize()


def filter(x):
    mat = re.search('[a-zA-Z]+', x)
    return mat.group() if mat else mat


ct.last_name = ct.last_name.apply(filter)
ct.first_name = ct.first_name.apply(filter)

ct.birthday = pd.to_datetime(ct.birthday)
ct.avail_date = pd.to_datetime(ct.avail_date)


def conform(x):
    return None if x in ['', 'N/A'] else x


bs.branch_name = bs.branch_name.apply(conform)
bs.price = bs.price.mask(bs.price.isna(), 0)

mm = pd.merge(ct, bs)

mm = mm[mm.avail_date < pd.Timestamp.today()]
mm = mm.drop(mm[mm.price == 0].index)
mm = mm.drop(mm[mm.branch_name.isna()].index)
mm = mm.drop(mm[mm.avail_date < mm.birthday].index)
mm = mm.drop_duplicates(['txn_id'])
mm
