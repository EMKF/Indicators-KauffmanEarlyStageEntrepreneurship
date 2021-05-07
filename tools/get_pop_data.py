# Experiment with index function
import joblib
import pandas as pd
import constants as c


def fetch_pre_2000_data(region):
    """Fetch population data for years: 1996 - 1999."""
    return pd.read_excel('http://www2.census.gov/library/publications/2011/compendia/statab/131ed/tables/12s0013.xls?', skiprows=3, skipfooter=9).\
        rename(columns={'State': 'region'}). \
        query('region not in ["Northeast", "Midwest", "South", "West "]') \
        [['region'] + [str(year) + " (July)" for year in range(1996, 2000)]].\
        query('region == "  United States "' if region == 'us' else 'region != "  United States "'). \
        rename(columns=lambda x: int(x[0:4]) if x != 'region' else x).\
        pipe(pd.melt, id_vars='region', value_vars=range(1996, 2000), var_name='time', value_name='population').\
        assign(
            region=lambda x: x.region.str.strip(),
            fips=lambda x: x.region.map(c.us_state_abbrev).map(c.state_abb_fips_dic),
            population=lambda x: x['population'] * 1000
        ).\
        astype({'time': 'int'}) \
        [['fips', 'region', 'time', 'population']]


def fetch_2020_data(region):
    """Fetch population data for 2020."""
    if region == 'us':
        return pd.read_excel('https://www2.census.gov/programs-surveys/popest/tables/2010-2019/national/totals/na-est2019-01.xlsx', skiprows=2, usecols=['Year and Month', 'Resident Population']).\
            iloc[128:139, :].\
            rename(columns={'Year and Month':'time', 'Resident Population': 'population'}).\
            query('time == ".July 1"').\
            assign(
                time=2020,
                fips='00',
                region='United States'
            ) \
            [['fips', 'region', 'time', 'population']]
    else:
        return pd.read_excel('https://www2.census.gov/programs-surveys/popest/tables/2010-2020/state/totals/nst-est2020.xlsx', skiprows=3, skipfooter=5).\
            rename(columns={'Unnamed: 0': 'region', 2020: 'population'}) \
            [['region', 'population']].\
            query('region not in ["Northeast", "Midwest", "South", "West", "United States"]').\
            dropna().\
            assign(
                region=lambda x: x.region.apply(lambda x: x.strip('.')),
                fips=lambda x: x.region.map(c.us_state_abbrev).map(c.state_abb_fips_dic),
                time=2020
            ) \
            [['fips', 'region', 'time', 'population']]


def fetch_data(region):
    """Fetch and merge 1990's and 2020 data for a given region ('us' or 'state')."""
    return fetch_pre_2000_data(region).\
        append(fetch_2020_data(region))
