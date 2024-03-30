from .datapipeline import get_data

import pandas as pd
from cleverminer import cleverminer

def correlate_population_product_types():
    df = get_data(force=True)

    result = cleverminer(
        df=df,
        target='YEAR',
        proc='CFMiner',
        quantifiers={'Base': 100, 'S_Up': 5},
        cond={
            'attributes': [
                {'name': 'COUNTRY', 'type': 'subset', 'minlen': 1, 'maxlen': 1},
                {'name': 'VALUE_CAT', 'type': 'subset', 'minlen': 1, 'maxlen': 1},
                {'name': 'PRODUCT', 'type': 'subset', 'minlen': 1, 'maxlen': 1},
                {'name': 'DENSITY_CAT', 'type': 'subset', 'minlen': 1, 'maxlen': 1},
            ],
            'minlen': 2,
            'maxlen': 4,
            'type': 'con',
        },
    )

    result.print_rulelist()

