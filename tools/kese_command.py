import os
import shutil
import joblib
import numpy as np
import pandas as pd
import constants as c
import kese_helpers as h
from kauffman.data_fetch import pep, bed


def _format_csv(df):
    return df.astype({'fips': 'str', 'time': 'int'})


def _fetch_data_cps(fetch_data):
    """
    Fetch CPS data from https://people.ucsc.edu/~rfairlie/data/microdata/. 
    Pre-process the data.

    Parameters
    ----------
    region: {'us', 'state'}
        Geographical level of the data.
    """
    print('Fetching CPS data')

    if fetch_data:
        df_us = pd.DataFrame()
        df_state = pd.DataFrame()
        for year in range(1996, 2022):
            df_in = pd.read_csv(
                'https://people.ucsc.edu/~rfairlie/data/microdata/kieadata'
                f'{year}.csv'
            )

            df_us = df_us.append(h.preprocess_cps(df_in, 'us'))
            df_state = df_state.append(h.preprocess_cps(df_in, 'state'))
    else:
        df_us = pd.read_csv(c.filenamer(f'data/raw_data/cps_us.csv')) \
            .pipe(_format_csv)
        df_state = pd.read_csv(c.filenamer(f'data/raw_data/cps_state.csv')) \
            .pipe(_format_csv)

    joblib.dump(df_us, c.filenamer(f'data/temp/cps_us.pkl'))
    joblib.dump(df_state, c.filenamer(f'data/temp/cps_state.pkl'))


def _fetch_data_bed(region, fetch_data):
    """
    Fetch raw BED data. Data comes from two tables: table 1bf and 7.

    Parameters
    ----------
    region: {'us', 'state'}
        Geographical level of data to be fetched.
    fetch_data: bool
        When true, code fetches the raw data from source; otherwise, it uses the
        data in data/raw_data.
    """
    if fetch_data:
        print(
            f'\ncreating datasets neb/data/temp/bed_table1_{region}.pkl'
            f' and neb/data/temp/bed_table7_{region}.pkl'
        )
        df_t1 = bed(
            series='establishment age and survival', table='1bf', 
            geo_level=region
        )

        df_t7 = bed(
                series='establishment age and survival', table=7, 
                geo_level=region
            ) \
            .rename(columns={'age': 'firm_age'}) \
            .assign(Lestablishments=lambda x: x['establishments'].shift(1))
    else:
        df_t1 = pd.read_csv(
                c.filenamer(f'data/raw_data/bed_table1_{region}.csv')
            ) \
            .pipe(_format_csv)
        df_t7 = pd.read_csv(
                c.filenamer(f'data/raw_data/bed_table7_{region}.csv')
            ) \
            .pipe(_format_csv)

    joblib.dump(df_t1, c.filenamer(f'data/temp/bed_table1_{region}.pkl'))
    joblib.dump(df_t7, c.filenamer(f'data/temp/bed_table7_{region}.pkl'))


def _fetch_data_pep(region, fetch_data):
    """
    Fetch raw PEP data.

    Parameters
    ----------
    region: {'us', 'state'}
        Geographical level of data to be fetched.
    fetch_data: bool
        When true, code fetches the raw data from source; otherwise, it uses the
        data in data/raw_data.
    """
    if fetch_data:
        print(f'\ncreating dataset neb/data/temp/pep_{region}.pkl')
        df = pep(region) \
            .rename(columns={'POP': 'population'}) \
            .astype({'time': 'int', 'population': 'int'}) \
            .query('time >= 2000') \
            .append(h.pep_pre_2000(region)) \
            .sort_values(['fips', 'region', 'time']) \
            .reset_index(drop=True)
    else:
        df = pd.read_csv(c.filenamer(f'data/raw_data/pep_{region}.csv')) \
            .pipe(_format_csv)

    joblib.dump(df, c.filenamer(f'data/temp/pep_{region}.pkl'))


def _raw_data_fetch(fetch_data):
    """
    Fetch raw CPS, BED, and PEP data.

    Parameters
    ----------
    fetch_data: bool
        When true, code fetches the raw data from source; otherwise, it uses the
        data in data/raw_data.
    """

    if os.path.isdir(c.filenamer('data/temp')):
        _raw_data_remove(remove_data=True)
    os.mkdir(c.filenamer('data/temp'))

    _fetch_data_cps(fetch_data)
    for region in ['us', 'state']:
        _fetch_data_bed(region, fetch_data)
        _fetch_data_pep(region, fetch_data)


def _raw_data_merge(region):
    """
    Merge CPS, BED, and PEP data for a given geographical level.

    Parameters
    ----------
    region: {'us', 'state'}
        Geographical level of the data.

    Returns
    -------
    DataFrame
        The merged raw data
    """

    # Prep CPS data
    df_cps = joblib.load(c.filenamer(f'data/temp/cps_{region}.pkl'))

    # Prep BED data
    df_bed1 = joblib.load(c.filenamer(f'data/temp/bed_table1_{region}.pkl')) \
        [['fips', 'time', 'opening_job_gains']]

    df_bed7 = joblib.load(c.filenamer(f'data/temp/bed_table7_{region}.pkl')) \
        .query('firm_age == 1') \
        [['fips', 'end_year', 'establishments', 'Lestablishments']] \
        .rename(columns={'end_year': 'time'})

    # Prep PEP data
    df_pop = joblib.load(c.filenamer(f'data/temp/pep_{region}.pkl')) \
        [['fips', 'time', 'population']]

    return df_cps \
        .merge(df_bed1, how='left', on=['time', 'fips']) \
        .merge(df_bed7, how='left', on=['time', 'fips']) \
        .merge(df_pop, how='left', on=['time', 'fips'])


def _index_create(df, region):
    """
    Generate the Kauffman index.

    Parameters
    ----------
    df: DataFrame
        The indicators data
    region: {'us', 'state'}
        Geographical level of the data.

    Returns
    -------
    DataFrame
        The original data plus the new index variable
    """
    if region == 'us':
        # Generate the means and standard deviations of the US-level data 
        # indicators
        df_us = df.query('1996 <= time <= 2015 and category == "Total"')
        us_means = df_us[['rne', 'ose', 'sjc', 'ssr']].mean()
        us_std = df_us[['rne', 'ose', 'sjc', 'ssr']].std()

        # Save this information to the temp folder for future use by the 
        # state-level index creation
        joblib.dump(us_means, c.filenamer('data/temp/us_means.pkl'))
        joblib.dump(us_std, c.filenamer('data/temp/us_std.pkl'))

    elif region == 'state':
        us_means = joblib.load(c.filenamer('data/temp/us_means.pkl'))
        us_std = joblib.load(c.filenamer('data/temp/us_std.pkl'))
    
    return df \
        .assign(
            ose_z = lambda x: (x['ose'] - us_means['ose']) / us_std['ose'],
            rne_z = lambda x: (x['rne'] - us_means['rne']) / us_std['rne'],
            sjc_z = lambda x: (x['sjc'] - us_means['sjc']) / us_std['sjc'],
            ssr_z = lambda x: (x['ssr'] - us_means['ssr']) / us_std['ssr'],
            zindex = lambda x: (
                ((x['ose_z'] + x['rne_z'] + x['sjc_z'] + x['ssr_z']) / 4) * 2
            )
        ) \
        .drop(columns=['ose_z', 'rne_z', 'sjc_z', 'ssr_z'])


def _indicators_create(df, region):
    """
    Calculate the remaining Kauffman indicators and the index.

    Parameters
    ----------
    df: DataFrame
        Raw merged data
    region: {'us', 'state'}
        Geographical level of the data.

    Returns
    -------
    DataFrame
        Indicators data
    """

    # 3 year trailing average of certains subsets of the data (RNE and OSE for 
    # state-level, OSE for non-total US-level)
    if region == 'state':
        df[['rne', 'ose']] = df.groupby(['fips'])[['rne', 'ose']] \
            .transform(lambda x: x.rolling(window=3).mean())
    else:   
        df.loc[df.category != 'Total', 'ose'] = df[df.category != 'Total'] \
            .groupby(['fips', 'category'])['ose'] \
            .transform(lambda x: x.rolling(window=3).mean())

    # Generate Startup Early Job Creation (SJC) and Startup Early Survival Rate
    # (SSR)
    df['sjc'] = df['opening_job_gains'] / (df['population'] / 1000)
    df['ssr'] = df['establishments'] / df['Lestablishments']

    # Remove SJC and SSR for non-total categories
    df.loc[df.category != 'Total', ['sjc', 'ssr']] = np.NaN

    # Create index variable
    df = _index_create(df, region)

    return df


def _final_data_transform(df):
    """Format the KESE data for download."""
    return df \
        .rename(columns={'region': 'name', 'time': 'year'}) \
        .sort_values(['fips', 'year', 'category']) \
        .reset_index(drop=True) \
        [[
            'fips', 'name', 'type', 'category', 'year', 'rne', 'ose', 'sjc', 
            'ssr', 'zindex'
        ]]


def _create_kese_data(region):
    """Transform raw KESE data to final format."""
    return _raw_data_merge(region) \
            .pipe(_indicators_create, region) \
            .pipe(_final_data_transform)


def _download_csv_save(df, aws_filepath):
    """Save download-version of data to a csv."""
    df.to_csv(c.filenamer('data/kcr_calc_2021_kese_download.csv'), index=False)
    if aws_filepath:
        df.to_csv(
            f'{aws_filepath}/kcr_calc_2021_kese_download.csv', index=False
        )
    return df


def _download_to_alley_formatter(df, outcome):
    """
    Format data of a given outcome to be suitable for upload to the Kauffman 
    website.

    Parameters
    ----------
    df: DataFrame
        The data to be formatted
    outcome: str
        The column name of the outcome whose values become the cells of the 
        dataframe

    Returns
    -------
    DataFrame
        Formatted data
    """

    return df[['fips', 'year', 'type', 'category'] + [outcome]] \
        .pipe(
            pd.pivot_table,
            index=['fips', 'type', 'category'],
            columns='year',
            values=outcome
        ) \
        .reset_index() \
        .replace('Total', '') \
        .rename(columns={
            'type': 'demographic-type', 'category': 'demographic', 
            'fips': 'region'
        })


def _website_csvs_save(df, aws_filepath):
    """Format and save csv of data to be uploaded to the website."""
    print(df.head())
    for indicator in ['rne', 'ose', 'sjc', 'ssr', 'zindex']:
        df_out = df.pipe(_download_to_alley_formatter, indicator)

        df_out.to_csv(
            c.filenamer(f'data/kcr_calc_2021_kese_website_{indicator}.csv'), 
            index=False
        )
        if aws_filepath:
            df_out.to_csv(
                f'{aws_filepath}/kcr_calc_2021_kese_website_{indicator}.csv', 
                index=False
            )


def _raw_data_remove(remove_data=True):
    """If remove_data set to "True", remove TEMP files."""
    if remove_data:
        shutil.rmtree(c.filenamer('data/temp'))  # remove unwanted files


def kese_data_create_all(raw_data_fetch, raw_data_remove, aws_filepath=None):
    """
    Create and save KESE data. This is the main function of kese_command.py. 

    Transform raw KESE data and save it to two csv's: One for user download, and
    one formatted for upload to the Kauffman site.

    Parameters
    ----------
    raw_data_fetch: bool
        When true, code fetches the raw data from source; otherwise, it uses the
        data in data/raw_data.
    raw_data_remove: bool
        Specifies whether to delete TEMP data at the end.
    aws_filepath: str
        If present, the AWS filepath at which to stash the data.
    """
    _raw_data_fetch(raw_data_fetch)

    pd.concat(
        [
            _create_kese_data(region) for region in ['us', 'state']
        ]
    ) \
        .pipe(_download_csv_save, aws_filepath) \
        .pipe(_website_csvs_save, aws_filepath)

    _raw_data_remove(raw_data_remove)


if __name__ == '__main__':
    kese_data_create_all(
        raw_data_fetch=False,
        raw_data_remove=True,
        # aws_filepath='s3://emkf.data.research/indicators/kese/data_outputs/kcr_kese_calculator'
    )
