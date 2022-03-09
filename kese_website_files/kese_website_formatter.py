import pandas as pd
import numpy as np
import sys
import tools.constants as c
from functools import reduce

pd.set_option('max_columns', 1000)
pd.set_option('max_info_columns', 1000)
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 30000)
pd.set_option('max_colwidth', 4000)
pd.set_option('display.float_format', lambda x: '%.8f' % x)
pd.set_option('chained_assignment',None)

def rne_data_create():
    # pull state and national
    state = pd.read_excel(f's3://emkf.data.research/indicators/kese/data_outputs/2021_kese_website/2021_rob_kese_files/Kauffman_Indicators_Data_State_1996_2021.xlsx', sheet_name='Rate of New Entrepreneurs')
    national = pd.read_excel(f's3://emkf.data.research/indicators/kese/data_outputs/2021_kese_website/2021_rob_kese_files/Kauffman_Indicators_Data_National_1996_2021.xlsx', sheet_name='Rate of New Entrepreneurs')
    # rename and create demographic columns
    national = national.rename(columns={"demtype": "demographic-type"})
    state.insert(1, 'demographic-type', np.nan)
    state.insert(2, 'demographic', np.nan)
    # subset columns
    national = national[national.columns[pd.Series(national.columns).str.startswith(('sname', 'demo', 'rne_'))]]
    state = state[state.columns[pd.Series(state.columns).str.startswith(('sname', 'demo', 'rne_'))]]
    # strip column names
    national.columns = national.columns.str.lstrip("rne_")
    national.columns = national.columns.str.lstrip("s")
    state.columns = state.columns.str.lstrip("rne_")
    state.columns = state.columns.str.lstrip("s")
    # concat national and state
    frames = [national, state]
    rne = pd.concat(frames)
    # rename region column and map fips codes
    rne['name'] = rne['name'].map(c.us_state_abbrev).map(c.state_abb_fips_dic)
    rne = rne.rename(columns={"name": "region"})
    # sort by demographic
    rne = rne.sort_values(by=['demographic-type']).reset_index(drop=True)
    # rne.to_csv(f's3://emkf.data.research/indicators/kese/data_outputs/2021_kese_website/kese_website_rne.csv', index=False)
    return rne

def ose_data_create():
    # pull state and national
    state = pd.read_excel('/Users/hmurray/Projects/kese/kese_website_files/Kauffman_Indicators_Data_State_1996_2021.xlsx', sheet_name='Opportunity Share of NE')
    national = pd.read_excel('/Users/hmurray/Projects/kese/kese_website_files/Kauffman_Indicators_Data_National_1996_2021.xlsx', sheet_name='Opportunity Share of NE')
    # rename and create demographic columns
    national = national.rename(columns={"demtype": "demographic-type"})
    state.insert(1, 'demographic-type', np.nan)
    state.insert(2, 'demographic', np.nan)
    # subset columns
    national = national[national.columns[pd.Series(national.columns).str.startswith(('sname', 'demo', 'ose_'))]]
    state = state[state.columns[pd.Series(state.columns).str.startswith(('sname', 'demo', 'ose_'))]]
    # strip column names
    national.columns = national.columns.str.lstrip("ose_")
    national.columns = national.columns.str.lstrip("s")
    state.columns = state.columns.str.lstrip("ose_")
    state.columns = state.columns.str.lstrip("s")
    # concat national and state
    frames = [national, state]
    ose = pd.concat(frames)
    # rename region column and map fips codes
    ose['name'] = ose['name'].map(c.us_state_abbrev).map(c.state_abb_fips_dic)
    ose = ose.rename(columns={"name": "region"})
    # sort by demographic
    ose = ose.sort_values(by=['demographic-type']).reset_index(drop=True)
    # ose.to_csv(f's3://emkf.data.research/indicators/kese/data_outputs/2021_kese_website/kese_website_ose.csv', index=False)
    return ose

def sjc_data_create():
    # pull state and national
    state = pd.read_excel(
        '/Users/hmurray/Projects/kese/kese_website_files/Kauffman_Indicators_Data_State_1996_2021.xlsx',
        sheet_name='Startup Job Creation')
    national = pd.read_excel(
        '/Users/hmurray/Projects/kese/kese_website_files/Kauffman_Indicators_Data_National_1996_2021.xlsx',
        sheet_name='Startup Job Creation')
    # rename and create demographic columns
    state.insert(1, 'demographic-type', np.nan)
    state.insert(2, 'demographic', np.nan)
    national.insert(1, 'demographic-type', np.nan)
    national.insert(2, 'demographic', np.nan)
    # subset columns
    national = national[national.columns[pd.Series(national.columns).str.startswith(('sname', 'demo', 'sjc_'))]]
    state = state[state.columns[pd.Series(state.columns).str.startswith(('sname', 'demo', 'sjc_'))]]
    # strip column names
    national.columns = national.columns.str.lstrip("sjc_")
    national.columns = national.columns.str.lstrip("s")
    state.columns = state.columns.str.lstrip("sjc_")
    state.columns = state.columns.str.lstrip("s")
    national = national[national.columns.drop(list(national.filter(regex='_jobs')))]
    state = state[state.columns.drop(list(state.filter(regex='_jobs')))]
    national = national[national.columns.drop(list(national.filter(regex='_pop')))]
    state = state[state.columns.drop(list(state.filter(regex='_pop')))]
    # concat national and state
    frames = [national, state]
    sjc = pd.concat(frames)
    # rename region column and map fips codes
    sjc['name'] = sjc['name'].map(c.us_state_abbrev).map(c.state_abb_fips_dic)
    sjc = sjc.rename(columns={"name": "region"})
    # change column types
    sjc = sjc.astype({'demographic-type': 'object', 'demographic': 'object'})
    # sort by demographic
    sjc = sjc.sort_values(by=['demographic-type']).reset_index(drop=True)
    # sjc.to_csv(f's3://emkf.data.research/indicators/kese/data_outputs/2021_kese_website/kese_website_sjc.csv', index=False)
    return sjc

def ssr_data_create():
    # pull state and national
    state = pd.read_excel(
        '/Users/hmurray/Projects/kese/kese_website_files/Kauffman_Indicators_Data_State_1996_2021.xlsx',
        sheet_name='Startup Survival Rate')
    national = pd.read_excel(
        '/Users/hmurray/Projects/kese/kese_website_files/Kauffman_Indicators_Data_National_1996_2021.xlsx',
        sheet_name='Startup Survival Rate')
    # rename and create demographic columns
    state.insert(1, 'demographic-type', np.nan)
    state.insert(2, 'demographic', np.nan)
    national.insert(1, 'demographic-type', np.nan)
    national.insert(2, 'demographic', np.nan)
    # subset columns
    national = national[national.columns[pd.Series(national.columns).str.startswith(('sname', 'demo', 'ssr_'))]]
    state = state[state.columns[pd.Series(state.columns).str.startswith(('sname', 'demo', 'ssr_'))]]
    national = national[national.columns.drop(list(national.filter(regex='_new')))]
    state = state[state.columns.drop(list(state.filter(regex='_new')))]
    national = national[national.columns.drop(list(national.filter(regex='_surv')))]
    state = state[state.columns.drop(list(state.filter(regex='_surv')))]
    # strip column names
    national.columns = national.columns.str.lstrip("ssr_")
    national.columns = national.columns.str.lstrip("s")
    state.columns = state.columns.str.lstrip("ssr_")
    state.columns = state.columns.str.lstrip("s")
    # concat national and state
    frames = [national, state]
    ssr = pd.concat(frames)
    # rename region column and map fips codes
    ssr['name'] = ssr['name'].map(c.us_state_abbrev).map(c.state_abb_fips_dic)
    ssr = ssr.rename(columns={"name": "region"})
    # change column types
    ssr = ssr.astype({'demographic-type': 'object', 'demographic': 'object'})
    # sort by demographic
    ssr = ssr.sort_values(by=['demographic-type']).reset_index(drop=True)
    # ssr.to_csv(f's3://emkf.data.research/indicators/kese/data_outputs/2021_kese_website/kese_website_ssr.csv', index=False)
    return ssr

def index_data_create():
    # pull state and national
    state = pd.read_excel(
        '/Users/hmurray/Projects/kese/kese_website_files/Kauffman_Indicators_Data_State_1996_2021.xlsx',
        sheet_name='KESE Index')
    national = pd.read_excel(
        '/Users/hmurray/Projects/kese/kese_website_files/Kauffman_Indicators_Data_National_1996_2021.xlsx',
        sheet_name='KESE Index')
    # subset columns
    national = national[national.columns[pd.Series(national.columns).str.startswith(('sname', 'demo', 'z'))]]
    state = state[state.columns[pd.Series(state.columns).str.startswith(('sname', 'demo', 'z'))]]
    # strip column names
    national.columns = national.columns.str.lstrip("zindex_")
    national.columns = national.columns.str.lstrip("s")
    state.columns = state.columns.str.lstrip("zindex_")
    state.columns = state.columns.str.lstrip("s")
    # rename and create demographic columns
    state.insert(1, 'demographic-type', np.nan)
    state.insert(2, 'demographic', np.nan)
    national.insert(1, 'demographic-type', np.nan)
    national.insert(2, 'demographic', np.nan)
    # concat national and state
    frames = [national, state]
    index = pd.concat(frames)
    # rename region column and map fips codes
    index['name'] = index['name'].map(c.us_state_abbrev).map(c.state_abb_fips_dic)
    index = index.rename(columns={"name": "region"})
    # change column types
    index = index.astype({'demographic-type': 'object', 'demographic': 'object'})
    # sort by demographic
    index = index.sort_values(by=['demographic-type']).reset_index(drop=True)
    # index.to_csv(f's3://emkf.data.research/indicators/kese/data_outputs/2021_kese_website/kese_website_zindex.csv', index=False)
    return index

def ind_data_download_create(df, name):
    # rename columns
    df = df.rename(columns={"region": "name", "demographic-type": "type", "demographic": "category"})
    # insert fips name column
    df.insert(0, 'fips', df['name'])
    # map fips codes to region names
    df['name'] = df['name'].map(c.state_fips_abb_dic).map(c.abbrev_us_state)
    # # wide to long
    df = df.melt(id_vars=['fips', 'name', 'type', 'category'], var_name='year', value_name=name)
    return df



def data_download(rne, ose, sjc, ssr, index):
    rneose = pd.merge(rne, ose, how='left', on=['fips', 'name', 'type', 'category', 'year']).reset_index(drop=True)
    sjcssr = pd.merge(sjc, ssr, how='left', on=['fips', 'name', 'type', 'category', 'year']).reset_index(drop=True)
    rneosesjcssr = pd.merge(rneose, sjcssr, how='left', on=['fips', 'name', 'type', 'category', 'year']).reset_index(drop=True)
    download = pd.merge(rneosesjcssr, index, how='left', on=['fips', 'name', 'type', 'category', 'year']).reset_index(drop=True)
    download = download.sort_values(by=['name', 'year']).reset_index(drop=True)
    print(download.head())
    # download.to_csv(f's3://emkf.data.research/indicators/kese/data_outputs/2021_kese_website/kese_download.csv', index=False)
    return download


if __name__ == '__main__':
    # website files
    rne = rne_data_create()
    ose = ose_data_create()
    sjc = sjc_data_create()
    ssr = ssr_data_create()
    index = index_data_create()
    # individual data download files
    rne = ind_data_download_create(rne, 'rne')
    ose = ind_data_download_create(ose, 'ose')
    sjc = ind_data_download_create(sjc, 'sjc')
    ssr = ind_data_download_create(ssr, 'ssr')
    index = ind_data_download_create(index, 'zindex')
    # data download
    download = data_download(rne, ose, sjc, ssr, index)

