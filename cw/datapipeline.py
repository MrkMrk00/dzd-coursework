import os
import pandas as pd
import sqlite3
import pprint

DB_FILE_NAME = 'database.sqlite'
CONNECTION: sqlite3.Connection | None = None
DATA_DIR = os.path.join(os.getcwd(), 'data')

PRODUCT_VARIANTS = {
    'Hydro': { 'renewable': True },
    'Wind': { 'renewable': True },
    'Solar': { 'renewable': True },
    'Geothermal': { 'renewable': True },
    'Total combustible fuels': { 'renewable': False },
    'Coal': { 'renewable': False },
    'Oil': { 'renewable': False },
    'Natural gas': { 'renewable': False },
    'Combustible renewables': { 'renewable': True },
    'Net electricity production': { 'renewable': None },
    'Electricity supplied': { 'renewable': None },
    'Used for pumped storage': { 'renewable': None },
    'Distribution losses': { 'renewable': None },
    'Final consumption': { 'renewable': None },
    'Renewables': { 'renewable': True },
    'Non-renewables': { 'renewable': False },
    'Others': { 'renewable': None },
    'Other renewables aggregated': { 'renewable': True },
    'Low carbon': { 'renewable': True },
    'Fossil fuels': {'renewable': False },
    'Other combustible non-renewables': { 'renewable': False },
    'Not specified': { 'renewable': False },
    'Total imports': { 'renewable': None },
    'Total exports': { 'renewable': None },
    'Electricity trade': { 'renewable': None },
    'Nuclear': { 'renewable': False },
    'Other renewables': { 'renewable': True },
}

def connection() -> sqlite3.Connection:
    global CONNECTION
    if CONNECTION is None:
        CONNECTION = sqlite3.connect(DB_FILE_NAME)
        CONNECTION.row_factory = sqlite3.Row

    return CONNECTION


def get_datafile(file_name: str) -> str:
    return os.path.join(DATA_DIR, file_name)


def _load_sql() -> None:
    db = connection()

    main = pd.read_csv(get_datafile('electricity.csv'))
    main.to_sql('electricity', con=db, if_exists='replace', index=False)

    population = pd.read_csv(get_datafile('population_total_long.csv'))
    population.to_sql('population', con=db, if_exists='replace', index=False)

    female_perc = pd.read_csv(get_datafile('population_female_percentage_long.csv'))
    female_perc.to_sql('female_perc', con=db, if_exists='replace', index=False)

    density = pd.read_csv(get_datafile('population_density_long.csv'))
    density.to_sql('density', con=db, if_exists='replace', index=False)

    over_65 = pd.read_csv(get_datafile('population_above_age_65_percentage_long.csv'))
    over_65.to_sql('over_65', con=db, if_exists='replace', index=False)

    under_14 = pd.read_csv(get_datafile('population_below_age_14_percentage_long.csv'))
    under_14.to_sql('under_14', con=db, if_exists='replace', index=False)


def get_data_sql(force: bool = False) -> None:
    if force or not os.path.exists(DB_FILE_NAME):
        _load_sql()

    cursor = connection().cursor()

    complete_query = """
        SELECT 
            e.*, 
            population.Count as POPULATION,
            female_perc.Count as FEMALE_PERCENTAGE,
            density.Count as POPULATION_DENSITY,
            under_14.Count as POPULATION_UNDER_14,
            over_65.Count as POPULATION_OVER_65

        FROM electricity e
            JOIN population ON (
                e.country = population."Country Name"
                    AND e.year = population."Year"
            )
            JOIN female_perc ON (
                e.country = female_perc."Country Name"
                    AND e.year = female_perc."Year"
            )
            JOIN density ON (
                e.country = density."Country Name"
                    AND e.year = density."Year"
            )
            JOIN over_65 ON (
                e.country = over_65."Country Name"
                    AND e.year = over_65."Year"
            )
            JOIN under_14 ON (
                e.country = under_14."Country Name"
                    AND e.year = under_14."Year"
            )

            WHERE e.YEAR <= 2018 AND e.YEAR >= 2010

            LIMIT 50
    """


    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint([ dict(x) for x in cursor.execute(complete_query).fetchall()])

    cursor.close()

