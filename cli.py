#!/usr/bin/env python3

import sys
from cw.datapipeline import get_data_sql
import pandas as pd

def generate_report() -> None:
    from pandas_cat import pandas_cat

    data = get_data_sql(force=True)
    df = pd.DataFrame(data)

    print(pandas_cat.profile(df=df,opts={"auto_prepare":True, 'cat_limit': 100}))
    

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'report':
        generate_report()
        exit(0)


