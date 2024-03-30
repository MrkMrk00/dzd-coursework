#!/usr/bin/env python3

import sys
from cw.datapipeline import get_data_sql, connection, get_electricity_produced
import pandas as pd
from pandas_cat import pandas_cat

def generate_report() -> None:
    data = get_data_sql(force=True)
    df = pd.DataFrame(data)

    print(pandas_cat.profile(df=df, dataset_name='', opts={"auto_prepare":True, 'cat_limit': 100}))
    

def generate_report_produced_by_year() -> None:
    data = get_electricity_produced()

    pandas_cat.profile(df=data, dataset_name='Electricity produced', opts={"auto_prepare":True, 'cat_limit': 100})


def create_result_table() -> None:
    data = get_data_sql(force=True)
    df = pd.DataFrame(data)

    df.to_sql('result', con=connection(), if_exists='replace', index=False)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'report':
        generate_report()
        exit(0)

    elif len(sys.argv) > 1 and sys.argv[1] == 'table':
        create_result_table()
        exit(0)

    elif len(sys.argv) > 1 and sys.argv[1] == 'report_produced':
        generate_report_produced_by_year()
        exit(0)

