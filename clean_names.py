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

ct.to_parquet('ct_duplicates_names.parquet')
bs.to_parquet('bs_duplicates_names.parquet')
