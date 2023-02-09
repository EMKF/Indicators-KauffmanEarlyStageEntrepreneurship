import pandas as pd
import constants as c


def _rne(df):
    """Calculate the Rate of New Entrepreneurs for a given region and year."""
    return (df['ent015ua'] * df['wgtat1']).sum() / df['wgtat1'].sum()


def _ose(df):
    """Calculate the Opportunity Share of Entrepreneurs for a given region and year."""
    return (df['oppshare'] * df['wgtat1']).sum() / df['wgtat1'].sum()


def preprocess_cps(df, region):
    """
    Pre-processes CPS data. Generate indicators and aggregate it to the annual level, broken down by
    category.

    Parameters
    ----------
    df : DataFrame
        Raw CPS data

    region : str
        Geographical level of data to be fetched. Options: 'us' or 'state'

    Returns
    -------
    DataFrame
        The processed data
    """
    df.query('yeart1 == yeart1', inplace=True)
    print('\tPre-processing data for', region, df['yeart1'].unique())

    if region == 'state':
        df_rne = df \
            [~df.ent015ua.isna()].\
            groupby(['yeart1', 'state']).apply(_rne).\
            reset_index(name='rne')\
            [['yeart1', 'state', 'rne']]

        df_ose = df \
            [~df.oppshare.isna()].\
            groupby(['yeart1', 'state']).apply(_ose).\
            reset_index(name='ose')\
            [['yeart1', 'state', 'ose']]

        df_processed = df_rne.\
            merge(df_ose).\
            assign(
                category='Total',
                type='Total',
                fips=lambda x: x.state.map(c.cps_to_fips),
                region=lambda x: x['fips'].map(c.state_fips_abb_dic).map(c.abbrev_us_state)
            ).\
            drop('state', 1)

    else:
        df_processed = pd.DataFrame()
        for type_c in c.kese_categories:
            for cat in c.kese_categories[type_c]:
                df_rne = df \
                    [~df.ent015ua.isna()].\
                    query(c.kese_category_queries[cat]).\
                    groupby('yeart1').apply(_rne).\
                    reset_index(name='rne') \
                    [['yeart1', 'rne']]

                df_ose = df \
                    [~df.oppshare.isna()].\
                    query(c.kese_category_queries[cat]).\
                    groupby('yeart1').apply(_ose).\
                    reset_index(name='ose') \
                    [['yeart1', 'ose']]

                df_processed = df_processed.\
                    append(
                        df_rne.\
                            merge(df_ose).\
                            assign(
                                type=type_c,
                                category=cat,
                                fips='00'
                            )
                    ).\
                    assign(region='United States')

    return df_processed. \
        rename(columns={'yeart1': 'time'}).\
        astype({'time': 'int'}) \
        [['fips', 'region', 'type', 'category', 'time', 'rne', 'ose']]


def pep_pre_2000(region):
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


def pep_2020(region):
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
            rename(columns={'Unnamed: 0': 'region', 'July 1': 'population'}) \
            [['region', 'population']].\
            query('region not in ["Northeast", "Midwest", "South", "West", "United States"]').\
            dropna().\
            assign(
                region=lambda x: x.region.apply(lambda x: x.strip('.')),
                fips=lambda x: x.region.map(c.us_state_abbrev).map(c.state_abb_fips_dic),
                time=2020
            ) \
            [['fips', 'region', 'time', 'population']]

def pep_2021(region):
    """Fetch population data for 2021."""
    if region == 'us':
        return pd.read_excel('https://www2.census.gov/programs-surveys/popest/tables/2020-2021/national/totals/NA-EST2021-POP.xlsx', skiprows=2, usecols=['Year and Month', 'Resident Population']).\
            iloc[11:23, :].\
            rename(columns={'Year and Month':'time', 'Resident Population': 'population'}).\
            query('time == ".July 1"').\
            assign(
                time=2021,
                fips='00',
                region='United States'
            ) \
            [['fips', 'region', 'time', 'population']]
    else:
        return pd.read_excel('https://www2.census.gov/programs-surveys/popest/tables/2020-2021/state/totals/NST-EST2021-POP.xlsx', skiprows=3, skipfooter=5).\
            rename(columns={'Unnamed: 0': 'region', 2021: 'population'}) \
            [['region', 'population']].\
            query('region not in ["Northeast", "Midwest", "South", "West", "United States"]').\
            dropna().\
            assign(
                region=lambda x: x.region.apply(lambda x: x.strip('.')),
                fips=lambda x: x.region.map(c.us_state_abbrev).map(c.state_abb_fips_dic),
                time=2021
            ) \
            [['fips', 'region', 'time', 'population']]
