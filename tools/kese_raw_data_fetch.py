import sys
import boto3
import joblib
import pandas as pd
import constants as c
import kese_helpers as h
from kauffman.data_fetch import bed, pep
from kauffman.tools import file_to_s3


def _cps():
    df_us = pd.DataFrame()
    df_state = pd.DataFrame()
    for year in range(1996, 2022):
        df_in = pd.read_csv(f'https://people.ucsc.edu/~rfairlie/data/microdata/kieadata{year}.csv')

        df_us = df_us.append(h.preprocess_cps(df_in, 'us'))
        df_state = df_state.append(h.preprocess_cps(df_in, 'state'))
    return df_us, df_state


def raw_data_update():
    joblib.dump(str(pd.to_datetime('today')), c.filenamer('data/raw_data/raw_data_fetch_time.pkl'))

    # CPS
    df_us, df_state = _cps()
    df_us.to_csv(c.filenamer(f'data/raw_data/cps_us.csv'), index=False)
    df_state.to_csv(c.filenamer(f'data/raw_data/cps_state.csv'), index=False)


    for region in ['us', 'state']:
        # BED
        bed(series='establishment age and survival', table='1bf', geo_level=region). \
            to_csv(c.filenamer(f'data/raw_data/bed_table1_{region}.csv'), index=False)

        bed(series='establishment age and survival', table=7, geo_level=region). \
            rename(columns={'age': 'firm_age'}). \
            assign(Lestablishments=lambda x: x['establishments'].shift(1)).\
            to_csv(c.filenamer(f'data/raw_data/bed_table7_{region}.csv'), index=False)

        # PEP
        pep(region). \
            rename(columns={'POP': 'population'}). \
            astype({'time': 'int', 'population': 'int'}). \
            query('time >= 2000'). \
            append(h.pep_pre_2000(region)). \
            sort_values(['fips', 'region', 'time']). \
            reset_index(drop=True).\
            to_csv(c.filenamer(f'data/raw_data/pep_{region}.csv'), index=False)


def s3_update():
    files_lst = [
        'raw_data_fetch_time.pkl', 'cps_us.csv', 'cps_state.csv', 'pep_us.csv', 'pep_state.csv',
        'bed_table1_us.csv', 'bed_table1_state.csv', 'bed_table7_us.csv', 'bed_table7_state.csv'
    ]

    for file in files_lst:
        file_to_s3(c.filenamer(f'data/raw_data/{file}'), 'emkf.data.research', f'indicators/kese/raw_data/{file}')


def main():
    raw_data_update()
    # s3_update()


if __name__ == '__main__':
    main()
