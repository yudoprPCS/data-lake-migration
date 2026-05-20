from elasticsearch import Elasticsearch
import elasticsearch.helpers as es_helper
from prophet import Prophet
import numpy as np
import pandas as pd
import datetime as date
import calendar
from dateutil.relativedelta import relativedelta
from hijri_converter import Hijri, Gregorian
import time
import json
import logging
import glob
import os
import shutil
import requests
import subprocess
import warnings
warnings.filterwarnings('ignore')

# untuk logging
class CurrDate:
    def __init__(self):
        self.curr_date = date.datetime.today() + date.timedelta(hours=7)
    
    def year_int(self): return self.curr_date.year
    def month_int(self): return self.curr_date.month
    def day_int(self): return self.curr_date.day
    def hour_int(self): return self.curr_date.hour
    def min_int(self): return self.curr_date.minute
    def sec_int(self): return self.curr_date.second
    def microsec_int(self): return self.curr_date.microsecond
    
    def year_str(self):
        return str(self.curr_date.year)
    def month_str(self):
        return '0' + str(self.curr_date.month) if len(str(self.curr_date.month)) == 1 else str(self.curr_date.month)
    def day_str(self):
        return '0' + str(self.curr_date.day) if len(str(self.curr_date.day)) == 1 else str(self.curr_date.day)
    def hour_str(self):
        return '0' + str(self.curr_date.hour) if len(str(self.curr_date.hour)) == 1 else str(self.curr_date.hour)
    def min_str(self):
        return '0' + str(self.curr_date.minute) if len(str(self.curr_date.minute)) == 1 else str(self.curr_date.minute)
    def sec_str(self):
        return '0' + str(self.curr_date.second) if len(str(self.curr_date.second)) == 1 else str(self.curr_date.second)
    def microsec_str(self):
        if len(str(self.curr_date.microsecond)) == 5:
            return '0' + str(self.curr_date.microsecond)
        elif len(str(self.curr_date.microsecond)) == 4:
            return '00' + str(self.curr_date.microsecond)
        elif len(str(self.curr_date.microsecond)) == 3:
            return '000' + str(self.curr_date.microsecond)
        elif len(str(self.curr_date.microsecond)) == 2:
            return '0000' + str(self.curr_date.microsecond)
        elif len(str(self.curr_date.microsecond)) == 1:
            return '00000' + str(self.curr_date.microsecond)
        else:
            return str(self.curr_date.microsecond)


# fbprophet nampilin log panjang yg ga perlu ditampilin
def remove_display_log_prophet():
    '''
    '''
    logger = logging.getLogger('cmdstanpy')
    logger.addHandler(logging.NullHandler())
    logger.propagate = False
    logger.setLevel(logging.CRITICAL)

# bikin directory di path tertentu
def makedir(path):
    '''
    path: string
    '''
    if not os.path.exists(path):
        os.makedirs(path)

# remove directory dari path tertentu
def rmdir(path):
    '''
    path: string
    '''
    shutil.rmtree(path)

# gabungin semua file csv di satu path directory tertentu
def concat_dfs(path):
    '''
    path: string
    '''
    
    file_lst = glob.glob('{}*'.format(path))
    tmp = []
    for i in range(len(file_lst)):
        tmp_df = pd.read_csv(file_lst[i], sep=';', dtype={'mid': 'str'})
        tmp.append(tmp_df)
    df = pd.concat(tmp)
    df = df.sort_values(by=['mid'])
    df = df.reset_index(drop=True)
    return df

# logging
def push_log(message):
    '''
    message: string
    '''
    
    curr_date = CurrDate()
    timestamp = '{}-{}-{} {}:{}:{}.{}'.format(curr_date.year_str(), curr_date.month_str(), curr_date.day_str(), curr_date.hour_str(), curr_date.min_str(), curr_date.sec_str(), curr_date.microsec_str())
    print('[{}] {}'.format(timestamp, message))
    log_timestamp.append(timestamp)
    log_message.append(message)


# https://docs.google.com/spreadsheets/d/1gilKM8MWOTDC5SCfYWO30xrC7vByXzeHcZ65Pmz7isA/edit?gid=0#gid=0

def connect_elasticsearch_read():
    """
    """
    
    es = Elasticsearch(
        ["https://m3sbridata-api.pcsindonesia.co.id"],
        api_key="xxx==",
        verify_certs=False
    )
    
    return es

def connect_elasticsearch_write():
    """
    """
    
    es = Elasticsearch(
        ["https://m3sbridata-api.pcsindonesia.co.id"],
        api_key="xxx==",
        verify_certs=False
    )
    
    return es

conn = connect_elasticsearch_read()

conn.info()


def ramadan_shawwal(curr_date=None):
    if curr_date is None:
        curr_date = date.datetime.today()

    curr_year = curr_date.year

    # --- Find next Ramadan (Month 9) ---
    hijri_year_9 = Gregorian(curr_year, 1, 1).to_hijri().year
    while True:
        hijri_date_9 = Hijri(hijri_year_9, 9, 1)
        g_obj_9 = hijri_date_9.to_gregorian()
        # Create standard datetime from Gregorian object attributes
        gregorian_date_9 = date.datetime(g_obj_9.year, g_obj_9.month, g_obj_9.day)
        
        if gregorian_date_9 > curr_date:
            break
        hijri_year_9 += 1

    # --- Find next Shawwal (Month 10) ---
    hijri_year_10 = Gregorian(curr_year, 1, 1).to_hijri().year
    while True:
        hijri_date_10 = Hijri(hijri_year_10, 10, 1)
        g_obj_10 = hijri_date_10.to_gregorian()
        # Create standard datetime from Gregorian object attributes
        gregorian_date_10 = date.datetime(g_obj_10.year, g_obj_10.month, g_obj_10.day)
        
        if gregorian_date_10 > curr_date:
            break
        hijri_year_10 += 1 

    # --- Action Logic ---
    next_month_date = curr_date + pd.DateOffset(months=1)
    two_months_date = curr_date + pd.DateOffset(months=2)
    
    # Use standard Python logic for comparison
    if (next_month_date.month == gregorian_date_9.month) or (next_month_date.month == gregorian_date_10.month):
        action = True
    elif (gregorian_date_10.day >= 20) and (two_months_date.month == (gregorian_date_10 + pd.DateOffset(months=2)).month):
        action = True
    else:
        action = False
    
    return {
        'curr_date': curr_date, 
        'ramadan_start': gregorian_date_9, 
        'shawwal_start': gregorian_date_10, 
        'action': action
    }


def get_population():
    '''
    '''
    
    push_log('Started Getting Population')
    
    es = connect_elasticsearch_read()
    
    query = {
        "_source": ["@timestamp", "ticket_install_id", "mid", "tid", "poi", "serial_number", "ticket_install_team", "ticket_latest_team", "merchant_name", "merchant_criteria", "store_code", "store_name", "store_address", "store_postal", "store_contact", "store_pic", "provinsi", "kab_kot", "kanwil", "installed_date", "is_production", "terminal_stok_id"],
        "runtime_mappings": {
            "is_pameran": {
                "type": "boolean",
                "script": {
                    "source": "if (doc['mid.keyword'].size() != 0){ def mid = doc['mid.keyword'].value; def mid_sub = mid.substring(5, 9); if (mid_sub == '1989'){ emit(true);} else { emit(false);}} else{ emit(false);}"
                }
            }
        },
        "script_fields": {
            "is_pameran": {
                "script": {
                    "source": "doc['is_pameran'].value;"
                }
            }
        },
        "query": {
            "bool": {
                "must": [
                    {
                        "exists": {
                            "field": "installed_date"
                        }
                    },
                    {
                        "term": {
                            "is_pameran": {
                                "value": "false"
                            }
                        }
                    }
                ],
                "must_not": [
                    {
                        "exists": {
                            "field": "ticket_dismantle_date_created"
                        }
                    }
                ]
            }
        }
    }

    result = es_helper.scan(es, query=query, index=['logstash-population_with_hb_trx_logon_ticket'], scroll='10m', size=1000)
    final_result = list(result)
    
    df = pd.json_normalize(final_result)
    df.columns = df.columns.str.replace('_source.', '')
    df = df[['@timestamp', 'ticket_install_id', 'mid', 'tid', 'poi', 'serial_number', 'ticket_install_team', 'ticket_latest_team', 'merchant_name', 'merchant_criteria', 'store_code', 'store_name', 'store_address', 'store_postal', 'store_contact', 'store_pic', 'provinsi', 'kab_kot', 'kanwil', 'installed_date', 'is_production', 'terminal_stok_id']]
    df = df.rename(columns={'ticket_install_id': 'id_sub_ticket'})
    
    df['@timestamp'] = df['@timestamp'].str[:10] + ' ' + df['@timestamp'].str[11:19]
    df['installed_date'] = df['installed_date'].str[:10] + ' ' + df['installed_date'].str[11:19]
    df['@timestamp'] = pd.to_datetime(df['@timestamp'], format='ISO8601')
    df['installed_date'] = pd.to_datetime(df['installed_date'], format='ISO8601')
    df['@timestamp'] = df['@timestamp'] + pd.Timedelta(hours=7)
    df['installed_date'] = df['installed_date'] + pd.Timedelta(hours=7)
    
    df = df.reset_index(drop=True)
    df.to_csv('/project/data/thermal_paper_prediction/mid_and_pop/pop.csv', sep=';', index=False)
    
    push_log('Finished Getting Population')


def get_mid():
    '''
    '''
    push_log('Started Getting MIDs')
    
    es = connect_elasticsearch_read()
    
    query = {
        'query': {
            'bool': {
                'must': [
                    {
                        'match_all': {}
                    }
                ]
            }
        }
    }

    result = es_helper.scan(es, query=query, index=['transform-transaction_summary_thermal_paper_unique_mid'])
    final_result = list(result)
    
    df = pd.json_normalize(final_result)
    df = df.rename(columns={'_source.mid': 'mid'})
    df = df[['mid']]
    df = df.sort_values(by=['mid'])
    df = df[df['mid'].notna()]
    df = df.reset_index(drop=True)
    
    df.to_csv('/project/data/thermal_paper_prediction/mid_and_pop/mid.csv', sep=';', index=False)
    
    push_log('Finished Getting MIDs')


def get_aggregated(default_size=10000000, num_partitions=10):
    '''
    '''
    
    push_log(f"Started Getting Aggregated Transaction MID Behavior. Partition Total: {num_partitions}")
    es = connect_elasticsearch_read()
    
    df_lst = []
    for i in range(num_partitions):
        query = {  
          "query": {
            "bool": {
              "filter": [
                {
                  "range": {
                    "created_date": {
                      "gte": "now-1M/M",
                      "lt": "now/d", 
                      "time_zone": "+07:00"
                    }
                  }
                }
              ]
            }
          },
          "aggs": {
            "terms_mid": {
              "terms": {
                "field": "mid.keyword",
                "size": default_size,
                "include": {
                  "partition": i,
                  "num_partitions": num_partitions
                }
              },
              "aggs": {
                "sum_of_print_count": {
                  "sum": {
                    "field": "sum_of_print_count"
                  }
                },
                "number_of_print_count": {
                  "sum": {
                    "field": "number_of_print_count"
                  }
                },
                "number_of_reprint": {
                  "sum": {
                    "field": "number_of_reprint"
                  }
                },
                "number_of_transaction": {
                  "sum": {
                    "field": "number_of_transaction"
                  }
                },
                "number_of_settlement": {
                    "sum": {
                        "field": "number_of_settlement"
                    }
                },
                "number_of_generate_qr": {
                    "sum": {
                        "field": "number_of_generate_qr"
                    }
                },
                "qr_is_included": {
                  "top_hits": {
                    "size": 1,
                    "_source": ["mid", "tm.qr_is_included"], 
                    "sort": [
                      {
                        "created_date": {
                          "order": "desc"
                        }
                      }
                    ]
                  }
                }
              }
            }
          },
          "size": 0
        }

        response = es.search(index=["transform-transaction_summary_thermal_paper_consumption*"], body=query)
        results = response["aggregations"]["terms_mid"]["buckets"]

        df = pd.json_normalize(results)
        df_lst.append(df)
        
        push_log(f"Partition {'00' + str(i+1) if len(str(i+1)) == 1 else '0' + str(i+1) if len(str(i+1)) == 2 else str(i+1)}/{num_partitions} Done")
        
    df_conc = pd.concat(df_lst)
    df_conc = df_conc.reset_index(drop=True)
    
    df_conc["qr_is_included"] = df_conc["qr_is_included.hits.hits"].str[0].str["_source"].str["tm"].str["qr_is_included"]
    df_conc = df_conc.rename(columns={"key": "mid",
                                      "doc_count": "record_count",
                                      "sum_of_print_count.value": "sum_of_print_count",
                                      "number_of_print_count.value": "number_of_print_count",
                                      "number_of_reprint.value": "number_of_reprint",
                                      "number_of_transaction.value": "number_of_transaction",
                                      "number_of_settlement.value": "number_of_settlement",
                                      "number_of_generate_qr.value": "number_of_generate_qr"})
    df_conc = df_conc.drop(["qr_is_included.hits.total.value", "qr_is_included.hits.total.relation", "qr_is_included.hits.max_score", "qr_is_included.hits.hits"], axis=1)
    df_conc["sum_of_print_count"] = df_conc["sum_of_print_count"].astype("int")
    df_conc["number_of_print_count"] = df_conc["number_of_print_count"].astype("int")
    df_conc["number_of_reprint"] = df_conc["number_of_reprint"].astype("int")
    df_conc["number_of_transaction"] = df_conc["number_of_transaction"].astype("int")
    df_conc["number_of_settlement"] = df_conc["number_of_settlement"].astype("int")
    df_conc["number_of_generate_qr"] = df_conc["number_of_generate_qr"].astype("int")
    df_conc["qr_is_included"] = df_conc["qr_is_included"].replace("9999", np.nan)
    
    df_conc.to_csv("/project/data/thermal_paper_prediction/additional_info/mid_behavior_aggregated.csv", sep=";", index=False)
    push_log("Finished Getting Aggregated Transaction MID Behavior")


def get_holiday_dates():
    '''
    '''
    push_log('Started Getting Holiday Dates')
    date_holiday = []

    # idul fitri
    for i in range(2020, 2076): # hijriah di library ini hanya sampai tahun 1500
        hijri_year = Gregorian(i, 1, 1).to_hijri().year
        hijri_date = Hijri(hijri_year, 10, 1)
        gregorian_date = hijri_date.to_gregorian()
        date_holiday.append(gregorian_date)

    idul_fitri = pd.DataFrame({
        'holiday': 'idul_fitri',
        'ds': date_holiday,
        'lower_window': -30,
        'upper_window': 14
    })


    holiday = pd.DataFrame()
    holiday = pd.concat([idul_fitri])
    holiday = holiday.sort_values(by=['ds'], ascending=True)

    holiday.to_csv('/project/data/thermal_paper_prediction/additional_info/holiday.csv', sep=';', index=False)
    push_log('Finished Getting Holiday Dates')


def get_global_trend(global_param=None, default_size=10000000, num_partitions=10, input_year=CurrDate().year_int(), input_month=CurrDate().month_int(), input_day=CurrDate().day_int(), predict_x_days=70, batch_size=1000, step_start=1):
    '''
    '''
    
    push_log('Started Getting Global Trend for {}'.format(global_param))
    
    if not global_param or global_param not in ['transaction_count', 'settlement_count', 'generate_qr_count']:
        push_log('Please pass one of these global_param as the function args: transaction_count, settlement_count, generate_qr_count')
        return None
    
    remove_display_log_prophet()
    
    push_log(f"Started Getting Aggregated Data. Partition Total: {num_partitions}")
    es = connect_elasticsearch_read()
    
    df_lst = []
    query = {
      "query": {
        "bool": {
          "filter": [
            {
              "range": {
                "created_date": {
                  # "gte": "now-12M/M",
                  "lt": "now+1M/M",
                  "time_zone": "+07:00"
                }
              }
            }
          ]
        }
      },
      "aggs": {
        "hist": {
          "date_histogram": {
            "field": "created_date",
            "calendar_interval": "day",
            "time_zone": "+07:00"
          },
          "aggs": {
            "sum_trx": {
              "sum": {
                "field": "number_of_transaction"
              }
            },
            "sum_settlement": {
              "sum": {
                "field": "number_of_settlement"
              }
            },
            "sum_gen_qr": {
              "sum": {
                "field": "number_of_generate_qr"
              }
            }
          }
        }
      },
      "size": 0, 
      "track_total_hits": "true"
    }

    response = es.search(index=["transform-transaction_summary_thermal_paper_consumption*"], body=query)
    results = response["aggregations"]["hist"]["buckets"]

    df = pd.json_normalize(results)
    df_lst.append(df)
        
    df_conc = pd.concat(df_lst)
    df_conc = df_conc.drop_duplicates()
    df_conc = df_conc.reset_index(drop=True)
    df_conc = df_conc.rename(columns={'key_as_string': 'ds'})
    df_conc['ds'] = pd.to_datetime(df_conc['ds'], format='ISO8601')
    
    push_log(f"Finished Getting Aggregated Data")
    
    
    
    #================================================================================================================================================#
    #================================================================ DEFINE DATES ==================================================================#
    #================================================================================================================================================#
    
    curr_year = input_year
    curr_month = input_month
    curr_day = input_day
    monthrange_max = calendar.monthrange(curr_year, curr_month)[1]

    sum_area_from_date = date.datetime(curr_year, curr_month, curr_day) + pd.Timedelta(days=monthrange_max-curr_day+1)
    if curr_month in [1]:
        if curr_year % 4 == 0:
            if curr_year % 100 == 0:
                if curr_year % 400 == 0:
                    sum_area_to_date = sum_area_from_date + pd.Timedelta(days=28)
                else:
                    sum_area_to_date = sum_area_from_date + pd.Timedelta(days=27)
            else:
                sum_area_to_date = sum_area_from_date + pd.Timedelta(days=28)
        else:
            sum_area_to_date = sum_area_from_date + pd.Timedelta(days=27)
    elif curr_month in [2, 4, 6, 7, 9, 11, 12]:
        sum_area_to_date = sum_area_from_date + pd.Timedelta(days=30)
    elif curr_month in [3, 5, 8, 10]:
        sum_area_to_date = sum_area_from_date + pd.Timedelta(days=29)
    
    sum_area_from_year = sum_area_from_date.year
    sum_area_from_month = sum_area_from_date.month
    sum_area_from_day = sum_area_from_date.day
    sum_area_from_year_str = str(sum_area_from_year)
    sum_area_from_month_str = '0' + str(sum_area_from_month) if len(str(sum_area_from_month)) == 1 else str(sum_area_from_month)
    sum_area_from_day_str = '0' + str(sum_area_from_day) if len(str(sum_area_from_day)) == 1 else str(sum_area_from_day)
    
    sum_area_to_year = sum_area_to_date.year
    sum_area_to_month = sum_area_to_date.month
    sum_area_to_day = sum_area_to_date.day
    sum_area_to_year_str = str(sum_area_to_year)
    sum_area_to_month_str = '0' + str(sum_area_to_month) if len(str(sum_area_to_month)) == 1 else str(sum_area_to_month)
    sum_area_to_day_str = '0' + str(sum_area_to_day) if len(str(sum_area_to_day)) == 1 else str(sum_area_to_day)
    
    max_train_date = date.datetime(curr_year, curr_month, curr_day) - pd.Timedelta(days=1)
    max_train_year = max_train_date.year
    max_train_month = max_train_date.month
    max_train_day = max_train_date.day
    max_train_year_str = str(max_train_year)
    max_train_month_str = '0' + str(max_train_month) if len(str(max_train_month)) == 1 else str(max_train_month)
    max_train_day_str = '0' + str(max_train_day) if len(str(max_train_day)) == 1 else str(max_train_day)
    
    last_month_date = max_train_date - relativedelta(months=1)
    last_month_date_start = date.datetime.strptime((max_train_date - relativedelta(months=1)).strftime('%Y-%m-01'), '%Y-%m-%d')
    last_month_date_end = date.datetime.strptime(last_month_date_start.strftime('%Y-%m-{}'.format(str(calendar.monthrange(last_month_date_start.year, last_month_date_start.month)[1]))), '%Y-%m-%d')
    curr_month_date_start = date.datetime.strptime(max_train_date.strftime('%Y-%m-01'), '%Y-%m-%d')
    curr_month_date_end = date.datetime.strptime(curr_month_date_start.strftime('%Y-%m-{}'.format(str(calendar.monthrange(curr_month_date_start.year, curr_month_date_start.month)[1]))), '%Y-%m-%d')
    
    append_max_date = max_train_date - pd.Timedelta(days=1)
    append_max_year = append_max_date.year
    append_max_month = append_max_date.month
    append_max_day = append_max_date.day
    append_max_year_str = str(append_max_year)
    append_max_month_str = '0' + str(append_max_month) if len(str(append_max_month)) == 1 else str(append_max_month)
    append_max_day_str = '0' + str(append_max_day) if len(str(append_max_day)) == 1 else str(append_max_day)
    
    push_log("Started Predicting Global Trend")
    hd = pd.read_csv('/project/data/thermal_paper_prediction/additional_info/holiday.csv', sep=';')
    hd['ds'] = pd.to_datetime(hd['ds'], format='%Y-%m-%d')
    
    df_05_prepare_forecast = df_conc.copy()
    if global_param == 'transaction_count':
        df_05_prepare_forecast['y'] = df_05_prepare_forecast['sum_trx.value'] / df_05_prepare_forecast['doc_count']
    elif global_param == 'settlement_count':
        df_05_prepare_forecast['y'] = df_05_prepare_forecast['sum_settlement.value'] / df_05_prepare_forecast['doc_count']
    elif global_param == 'generate_qr_count':
        df_05_prepare_forecast['y'] = df_05_prepare_forecast['sum_gen_qr.value'] / df_05_prepare_forecast['doc_count']
    
    df_05_prepare_forecast = df_05_prepare_forecast[['ds', 'y']]
    df_05_prepare_forecast = df_05_prepare_forecast[df_05_prepare_forecast['ds'] <= max_train_date] # train data
    
    model = Prophet(holidays=hd)
    model.fit(df_05_prepare_forecast)
    future = model.make_future_dataframe(periods=predict_x_days)
    df_06_predicted = model.predict(future)
    df_06_predicted = df_06_predicted.reset_index(drop=True)
    # df_06_predicted = df_06_predicted[(df_06_predicted['ds'] >= sum_area_from_date) & (df_06_predicted['ds'] <= sum_area_to_date)]
    push_log("Finished Predicting Global Trend")
    
    df_final = df_06_predicted.copy()
    df_final.to_csv('/project/data/thermal_paper_prediction/additional_info/global_trends/global_trend-{}{}-{}.csv'.format(sum_area_to_year_str, sum_area_to_month_str, global_param[:-6]), sep=';', index=False)
    
    push_log('Finished Getting Global Trend for {}'.format(global_param))
    
    # return df_final, model


def display_per_mid(custom_mid=None, sample_size=10, global_param=None, input_year=CurrDate().year_int(), input_month=CurrDate().month_int(), input_day=CurrDate().day_int(), predict_x_days=70, batch_size=1000, step_start=1):
    '''
    custom_mid: string mid
    global_param: string: transaction_count, settlement_count, generate_qr_count
    input_year: datetime year
    input_month: datetime month
    input_day: datetime day
    predict_x_days: int
    batch_size: int
    step_start: int
    '''
    
    if not global_param or global_param not in ['transaction_count', 'settlement_count', 'generate_qr_count']:
        push_log('Please pass one of these global_param as the function args: transaction_count, settlement_count, generate_qr_count')
        return None
    
    remove_display_log_prophet()
    
    # push_log('Started Grand Loop for {}'.format(global_param))
    
    es = connect_elasticsearch_read()
    
    #================================================================================================================================================#
    #================================================================ DEFINE DATES ==================================================================#
    #================================================================================================================================================#
    
    curr_year = input_year
    curr_month = input_month
    curr_day = input_day
    monthrange_max = calendar.monthrange(curr_year, curr_month)[1]

    sum_area_from_date = date.datetime(curr_year, curr_month, curr_day) + pd.Timedelta(days=monthrange_max-curr_day+1)
    if curr_month in [1]:
        if curr_year % 4 == 0:
            if curr_year % 100 == 0:
                if curr_year % 400 == 0:
                    sum_area_to_date = sum_area_from_date + pd.Timedelta(days=28)
                else:
                    sum_area_to_date = sum_area_from_date + pd.Timedelta(days=27)
            else:
                sum_area_to_date = sum_area_from_date + pd.Timedelta(days=28)
        else:
            sum_area_to_date = sum_area_from_date + pd.Timedelta(days=27)
    elif curr_month in [2, 4, 6, 7, 9, 11, 12]:
        sum_area_to_date = sum_area_from_date + pd.Timedelta(days=30)
    elif curr_month in [3, 5, 8, 10]:
        sum_area_to_date = sum_area_from_date + pd.Timedelta(days=29)
    
    sum_area_from_year = sum_area_from_date.year
    sum_area_from_month = sum_area_from_date.month
    sum_area_from_day = sum_area_from_date.day
    sum_area_from_year_str = str(sum_area_from_year)
    sum_area_from_month_str = '0' + str(sum_area_from_month) if len(str(sum_area_from_month)) == 1 else str(sum_area_from_month)
    sum_area_from_day_str = '0' + str(sum_area_from_day) if len(str(sum_area_from_day)) == 1 else str(sum_area_from_day)
    
    sum_area_to_year = sum_area_to_date.year
    sum_area_to_month = sum_area_to_date.month
    sum_area_to_day = sum_area_to_date.day
    sum_area_to_year_str = str(sum_area_to_year)
    sum_area_to_month_str = '0' + str(sum_area_to_month) if len(str(sum_area_to_month)) == 1 else str(sum_area_to_month)
    sum_area_to_day_str = '0' + str(sum_area_to_day) if len(str(sum_area_to_day)) == 1 else str(sum_area_to_day)
    
    max_train_date = date.datetime(curr_year, curr_month, curr_day) - pd.Timedelta(days=1)
    max_train_year = max_train_date.year
    max_train_month = max_train_date.month
    max_train_day = max_train_date.day
    max_train_year_str = str(max_train_year)
    max_train_month_str = '0' + str(max_train_month) if len(str(max_train_month)) == 1 else str(max_train_month)
    max_train_day_str = '0' + str(max_train_day) if len(str(max_train_day)) == 1 else str(max_train_day)
    
    last_month_date = max_train_date - relativedelta(months=1)
    last_month_date_start = date.datetime.strptime((max_train_date - relativedelta(months=1)).strftime('%Y-%m-01'), '%Y-%m-%d')
    last_month_date_end = date.datetime.strptime(last_month_date_start.strftime('%Y-%m-{}'.format(str(calendar.monthrange(last_month_date_start.year, last_month_date_start.month)[1]))), '%Y-%m-%d')
    curr_month_date_start = date.datetime.strptime(max_train_date.strftime('%Y-%m-01'), '%Y-%m-%d')
    curr_month_date_end = date.datetime.strptime(curr_month_date_start.strftime('%Y-%m-{}'.format(str(calendar.monthrange(curr_month_date_start.year, curr_month_date_start.month)[1]))), '%Y-%m-%d')
    
    append_max_date = max_train_date - pd.Timedelta(days=1)
    append_max_year = append_max_date.year
    append_max_month = append_max_date.month
    append_max_day = append_max_date.day
    append_max_year_str = str(append_max_year)
    append_max_month_str = '0' + str(append_max_month) if len(str(append_max_month)) == 1 else str(append_max_month)
    append_max_day_str = '0' + str(append_max_day) if len(str(append_max_day)) == 1 else str(append_max_day)
    
    rs_date = ramadan_shawwal()
    ramadan_start = rs_date['ramadan_start']
    shawwal_start = rs_date['shawwal_start']
    is_ramadan_shawwal = rs_date['action']
    
    #================================================================================================================================================#
    #=================================================================== GET MID ====================================================================#
    #================================================================================================================================================#
    
    mid = pd.read_csv('/project/data/thermal_paper_prediction/mid_and_pop/mid.csv', sep=';', dtype='str')
    all_mid = mid.copy()
    if custom_mid:
        all_mid = all_mid[all_mid['mid'] == custom_mid]
    else:
        all_mid = all_mid.sample(sample_size)
    all_mid = list(all_mid['mid'])
    len_mid = len(all_mid)
    
    hd = pd.read_csv('/project/data/thermal_paper_prediction/additional_info/holiday.csv', sep=';')
    hd['ds'] = pd.to_datetime(hd['ds'], format='%Y-%m-%d')
    
    #================================================================================================================================================#
    #================================================================ THE GRAND LOOP ================================================================#
    #================================================================================================================================================#
    
    push_log('Next Shawwal (Eid) will Start On {}'.format(shawwal_start))
    
    step_total = int(np.ceil(len(all_mid) / batch_size))
    # push_log('Started Predicting Thermal Paper Usage. Train Date: LTE {}-{}-{}. Predict From: {}-{}-{}. Predict To: {}-{}-{}. Batch Size: {}. MID Total: {}. Step Total: {}. Step Start: {}'.format(max_train_year_str, max_train_month_str, max_train_day_str, sum_area_from_year_str, sum_area_from_month_str, sum_area_from_day_str, sum_area_to_year_str, sum_area_to_month_str, sum_area_to_day_str, batch_size, len(all_mid), step_total, step_start))
    push_log('Displaying mid {}'.format(all_mid))
    
    sum_choice = [i for i in range(50, 76, 1)] # [50, 51, 52, ..., 75]
    # sum_choice = [50]
        
    start_time_step = time.time()
    start_time_total = time.time()
    step_count = step_start - 1
    
    if step_start == 1:
        init = batch_size
    else:
        init = step_start * batch_size  
    
    for step_start, step_end in zip(range(init - batch_size, len(all_mid), batch_size), range(init, len(all_mid) + batch_size, batch_size)):
        df_lst = []
        mid_step = all_mid[step_start:step_end]

        for mid in mid_step:
            # push_log(mid)
            query = {
                'query': {
                    'bool': {
                        'must': [
                            {
                                'term': {
                                    'mid': {
                                        'value': mid
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            result = list(es_helper.scan(es, query=query, index=['transform-transaction_summary_thermal_paper_consumption*']))
            if len(result) == 0:
                push_log('No Record Found for MID {}. Continuing...'.format(mid))
                continue
            df_01_raw_elk = pd.json_normalize(result)
            df_01_raw_elk['_source.created_date'] = pd.to_datetime(df_01_raw_elk['_source.created_date'], format='ISO8601')
            df_01_raw_elk = df_01_raw_elk[df_01_raw_elk['_source.created_date'] <= max_train_date]
            df_01_raw_elk['day_count'] = 1

            df_tmp_append_max_date = pd.DataFrame()
            df_tmp_append_max_date['_source.created_date'] = ['{}-{}-{} 17:00:00'.format(append_max_year_str, append_max_month_str, append_max_day_str)] # max date to train data
            df_tmp_append_max_date['_source.mid'] = [mid]
            df_tmp_append_max_date['_source.number_of_transaction'] = 0.0
            df_tmp_append_max_date['_source.number_of_settlement'] = 0.0
            df_tmp_append_max_date['_source.number_of_generate_qr'] = 0.0
            # df_tmp_append_max_date['_source.paper_consumption'] = 0.0
            df_tmp_append_max_date['_source.created_date'] = pd.to_datetime(df_tmp_append_max_date['_source.created_date'], format='ISO8601')
            df_tmp_append_max_date['_source.created_date'] = df_tmp_append_max_date['_source.created_date'] + pd.Timedelta(hours=7)

            df_02_variable_count = df_01_raw_elk.copy()
            df_02_variable_count['_source.created_date'] = df_02_variable_count['_source.created_date'] + pd.Timedelta(hours=7)
            if list(df_tmp_append_max_date['_source.created_date'].dt.strftime('%Y-%m-%d'))[0] not in list(df_02_variable_count['_source.created_date'].dt.strftime('%Y-%m-%d')):
                df_02_variable_count = pd.concat([df_02_variable_count, df_tmp_append_max_date])
            df_02_variable_count = df_02_variable_count.sort_values(by=['_source.created_date'], ascending=True)
            df_02_variable_count = df_02_variable_count.reset_index(drop=True)
            
            if global_param == 'transaction_count':
                df_02_variable_count = df_02_variable_count.rename(columns={
                    '_source.mid': 'mid',
                    '_source.number_of_transaction': 'y',
                    '_source.number_of_settlement': 'settlement_count',
                    '_source.number_of_generate_qr': 'generate_qr_count',
                    # '_source.paper_consumption': 'paper_consumption',
                    '_source.created_date': 'ds'
                })
                df_02_variable_count = df_02_variable_count[['ds', 'mid', 'settlement_count', 'generate_qr_count', 'y', 'day_count']]
                df_03_inject_date = df_02_variable_count.copy()
                df_03_inject_date.index = df_03_inject_date['ds']
                df_03_inject_date = df_03_inject_date.asfreq('D')
                df_03_inject_date = df_03_inject_date.drop(['ds'], axis=1)
                df_03_inject_date = df_03_inject_date.reset_index()
                df_03_inject_date['mid'] = df_03_inject_date['mid'].unique()[0]
                # df_03_inject_date['paper_consumption'] = df_03_inject_date['paper_consumption'].replace(np.nan, 0)
                df_03_inject_date['settlement_count'] = df_03_inject_date['settlement_count'].replace(np.nan, 0)
                df_03_inject_date['generate_qr_count'] = df_03_inject_date['generate_qr_count'].replace(np.nan, 0)
                df_03_inject_date['y'] = df_03_inject_date['y'].replace(np.nan, 0)
                
            elif global_param == 'settlement_count':
                df_02_variable_count = df_02_variable_count.rename(columns={
                    '_source.mid': 'mid',
                    '_source.number_of_transaction': 'transaction_count',
                    '_source.number_of_settlement': 'y',
                    '_source.number_of_generate_qr': 'generate_qr_count',
                    # '_source.paper_consumption': 'paper_consumption',
                    '_source.created_date': 'ds'
                })
                df_02_variable_count = df_02_variable_count[['ds', 'mid', 'y', 'generate_qr_count', 'transaction_count', 'day_count']]
                df_03_inject_date = df_02_variable_count.copy()
                df_03_inject_date.index = df_03_inject_date['ds']
                df_03_inject_date = df_03_inject_date.asfreq('D')
                df_03_inject_date = df_03_inject_date.drop(['ds'], axis=1)
                df_03_inject_date = df_03_inject_date.reset_index()
                df_03_inject_date['mid'] = df_03_inject_date['mid'].unique()[0]
                # df_03_inject_date['paper_consumption'] = df_03_inject_date['paper_consumption'].replace(np.nan, 0)
                df_03_inject_date['transaction_count'] = df_03_inject_date['transaction_count'].replace(np.nan, 0)
                df_03_inject_date['generate_qr_count'] = df_03_inject_date['generate_qr_count'].replace(np.nan, 0)
                df_03_inject_date['y'] = df_03_inject_date['y'].replace(np.nan, 0)
                
            elif global_param == 'generate_qr_count':
                df_02_variable_count = df_02_variable_count.rename(columns={
                    '_source.mid': 'mid',
                    '_source.number_of_transaction': 'transaction_count',
                    '_source.number_of_settlement': 'settlement_count',
                    '_source.number_of_generate_qr': 'y',
                    # '_source.paper_consumption': 'paper_consumption',
                    '_source.created_date': 'ds'
                })
                df_02_variable_count = df_02_variable_count[['ds', 'mid', 'settlement_count', 'y', 'transaction_count', 'day_count']]
                df_03_inject_date = df_02_variable_count.copy()
                df_03_inject_date.index = df_03_inject_date['ds']
                df_03_inject_date = df_03_inject_date.asfreq('D')
                df_03_inject_date = df_03_inject_date.drop(['ds'], axis=1)
                df_03_inject_date = df_03_inject_date.reset_index()
                df_03_inject_date['mid'] = df_03_inject_date['mid'].unique()[0]
                # df_03_inject_date['paper_consumption'] = df_03_inject_date['paper_consumption'].replace(np.nan, 0)
                df_03_inject_date['transaction_count'] = df_03_inject_date['transaction_count'].replace(np.nan, 0)
                df_03_inject_date['settlement_count'] = df_03_inject_date['settlement_count'].replace(np.nan, 0)
                df_03_inject_date['y'] = df_03_inject_date['y'].replace(np.nan, 0)

            df_04_filter_case = df_03_inject_date.copy()
            if df_04_filter_case['ds'].min() > max_train_date - pd.Timedelta(days=29): # exclude mids that have less than 30 days worth of transaction
                df_05_special_case = pd.DataFrame()
                df_05_special_case['mid'] = [mid]
                df_05_special_case['historical_sum_actual_all'] = df_03_inject_date['y'][df_03_inject_date['ds'] <= max_train_date].sum()
                df_05_special_case['historical_sum_actual_last_one_month'] = df_03_inject_date['y'][(df_03_inject_date['ds'] >= last_month_date) & (df_03_inject_date['ds'] <= max_train_date)].sum()
                df_05_special_case['historical_day_first_log'] = df_04_filter_case['ds'][df_04_filter_case['ds'] <= max_train_date].min()
                df_05_special_case['historical_day_last_log'] = df_01_raw_elk['_source.created_date'][df_01_raw_elk['_source.created_date'] <= max_train_date].max()
                df_05_special_case['historical_day_active'] = df_02_variable_count['day_count'][df_02_variable_count['ds'] <= max_train_date].sum()

                df_05_special_case['historical_day_first_log'] = pd.to_datetime(df_05_special_case['historical_day_first_log'], format=('%Y-%m-%d'))
                df_05_special_case['historical_day_last_log'] = pd.to_datetime(df_05_special_case['historical_day_last_log'], format=('%Y-%m-%d %H:%M:%S'))
                df_05_special_case['historical_day_last_log'] = df_05_special_case['historical_day_last_log'] + pd.Timedelta(hours=7)

                adjusted_roll_diff_days = (max_train_date - df_04_filter_case['ds'].min()).days + 1
                if adjusted_roll_diff_days < 1:
                    adjusted_roll_diff_days = 1
                df_05_special_case['sum_actual_special'] = df_05_special_case['historical_sum_actual_all'] * (30 / adjusted_roll_diff_days) # prorate

                for i in sum_choice:
                    df_05_special_case['sum_{}'.format(i)] = df_05_special_case['sum_actual_special']
                df_05_special_case['tag'] = ['special_case']

                df_05_special_case = df_05_special_case.drop(['sum_actual_special'], axis=1)

                df_lst.append(df_05_special_case)

            else: 
                df_05_prepare_forecast = df_04_filter_case.copy()
                df_05_prepare_forecast = df_05_prepare_forecast[['ds', 'y']]
                df_05_prepare_forecast = df_05_prepare_forecast[df_05_prepare_forecast['ds'] <= max_train_date] # train data

                model = Prophet(holidays=hd)
                model.fit(df_05_prepare_forecast)
                future = model.make_future_dataframe(periods=predict_x_days)
                df_06_predicted = model.predict(future)
                
                fig = model.plot(df_06_predicted)
                fig.suptitle(mid)
                
                fig2 = model.plot_components(df_06_predicted)
                fig2.suptitle(mid)
                
            df_lst.append(df_05_prepare_forecast)
        
        df_steps = pd.concat(df_lst)
        df_steps = df_steps.reset_index(drop=True)
        
    # return df_steps


def grand_loop(global_param=None, specify_path=None, input_year=CurrDate().year_int(), input_month=CurrDate().month_int(), input_day=CurrDate().day_int(), predict_x_days=70, batch_size=1000, step_start=1):
    '''
    global_param: string: transaction_count, settlement_count, generate_qr_count
    input_year: datetime year
    input_month: datetime month
    input_day: datetime day
    predict_x_days: int
    batch_size: int
    step_start: int
    '''
    
    if not global_param or global_param not in ['transaction_count', 'settlement_count', 'generate_qr_count']:
        push_log('Please pass one of these global_param as the function args: transaction_count, settlement_count, generate_qr_count')
        return None
    
    remove_display_log_prophet()
    
    push_log('Started Grand Loop for {}'.format(global_param))
    
    es = connect_elasticsearch_read()
    
    #================================================================================================================================================#
    #================================================================ DEFINE DATES ==================================================================#
    #================================================================================================================================================#
    
    curr_year = input_year
    curr_month = input_month
    curr_day = input_day
    monthrange_max = calendar.monthrange(curr_year, curr_month)[1]

    sum_area_from_date = date.datetime(curr_year, curr_month, curr_day) + pd.Timedelta(days=monthrange_max-curr_day+1)
    if curr_month in [1]:
        if curr_year % 4 == 0:
            if curr_year % 100 == 0:
                if curr_year % 400 == 0:
                    sum_area_to_date = sum_area_from_date + pd.Timedelta(days=28)
                else:
                    sum_area_to_date = sum_area_from_date + pd.Timedelta(days=27)
            else:
                sum_area_to_date = sum_area_from_date + pd.Timedelta(days=28)
        else:
            sum_area_to_date = sum_area_from_date + pd.Timedelta(days=27)
    elif curr_month in [2, 4, 6, 7, 9, 11, 12]:
        sum_area_to_date = sum_area_from_date + pd.Timedelta(days=30)
    elif curr_month in [3, 5, 8, 10]:
        sum_area_to_date = sum_area_from_date + pd.Timedelta(days=29)
    
    sum_area_from_year = sum_area_from_date.year
    sum_area_from_month = sum_area_from_date.month
    sum_area_from_day = sum_area_from_date.day
    sum_area_from_year_str = str(sum_area_from_year)
    sum_area_from_month_str = '0' + str(sum_area_from_month) if len(str(sum_area_from_month)) == 1 else str(sum_area_from_month)
    sum_area_from_day_str = '0' + str(sum_area_from_day) if len(str(sum_area_from_day)) == 1 else str(sum_area_from_day)
    
    sum_area_to_year = sum_area_to_date.year
    sum_area_to_month = sum_area_to_date.month
    sum_area_to_day = sum_area_to_date.day
    sum_area_to_year_str = str(sum_area_to_year)
    sum_area_to_month_str = '0' + str(sum_area_to_month) if len(str(sum_area_to_month)) == 1 else str(sum_area_to_month)
    sum_area_to_day_str = '0' + str(sum_area_to_day) if len(str(sum_area_to_day)) == 1 else str(sum_area_to_day)
    
    max_train_date = date.datetime(curr_year, curr_month, curr_day) - pd.Timedelta(days=1)
    max_train_year = max_train_date.year
    max_train_month = max_train_date.month
    max_train_day = max_train_date.day
    max_train_year_str = str(max_train_year)
    max_train_month_str = '0' + str(max_train_month) if len(str(max_train_month)) == 1 else str(max_train_month)
    max_train_day_str = '0' + str(max_train_day) if len(str(max_train_day)) == 1 else str(max_train_day)
    
    last_month_date = max_train_date - relativedelta(months=1)
    last_month_date_start = date.datetime.strptime((max_train_date - relativedelta(months=1)).strftime('%Y-%m-01'), '%Y-%m-%d')
    last_month_date_end = date.datetime.strptime(last_month_date_start.strftime('%Y-%m-{}'.format(str(calendar.monthrange(last_month_date_start.year, last_month_date_start.month)[1]))), '%Y-%m-%d')
    curr_month_date_start = date.datetime.strptime(max_train_date.strftime('%Y-%m-01'), '%Y-%m-%d')
    curr_month_date_end = date.datetime.strptime(curr_month_date_start.strftime('%Y-%m-{}'.format(str(calendar.monthrange(curr_month_date_start.year, curr_month_date_start.month)[1]))), '%Y-%m-%d')
    
    append_max_date = max_train_date - pd.Timedelta(days=1)
    append_max_year = append_max_date.year
    append_max_month = append_max_date.month
    append_max_day = append_max_date.day
    append_max_year_str = str(append_max_year)
    append_max_month_str = '0' + str(append_max_month) if len(str(append_max_month)) == 1 else str(append_max_month)
    append_max_day_str = '0' + str(append_max_day) if len(str(append_max_day)) == 1 else str(append_max_day)
    
    rs_date = ramadan_shawwal()
    ramadan_start = rs_date['ramadan_start']
    shawwal_start = rs_date['shawwal_start']
    is_ramadan_shawwal = rs_date['action']
    
    #================================================================================================================================================#
    #=========================================================== CREATE A NEW DIRECTORY =============================================================#
    #================================================================================================================================================#
    
    if not specify_path:
        dir_name = 'thermal_paper_lte_{}{}{}_sum_area_{}{}{}_to_{}{}{}'.format(
            max_train_year_str,
            max_train_month_str,
            max_train_day_str,
            sum_area_from_year_str,
            sum_area_from_month_str,
            sum_area_from_day_str,
            sum_area_to_year_str,
            sum_area_to_month_str,
            sum_area_to_day_str
        )
    else:
        dir_name = specify_path
    global_dir_name.append(dir_name)
    
    push_log('Started Creating a New Directory {}'.format(dir_name))
    makedir('/project/data/thermal_paper_prediction/{}'.format(dir_name))
    makedir('/project/data/thermal_paper_prediction/{}/steps_batch_size_{}_param_{}'.format(dir_name, batch_size, global_param))
    push_log('Finished Creating a New Directory {}'.format(dir_name))
    
    #================================================================================================================================================#
    #=================================================================== GET MID ====================================================================#
    #================================================================================================================================================#
    
    mid = pd.read_csv('/project/data/thermal_paper_prediction/mid_and_pop/mid.csv', sep=';', dtype='str')
    pop = pd.read_csv('/project/data/thermal_paper_prediction/mid_and_pop/pop.csv', sep=';', dtype='str')
    pop = pop[['mid']]
    pop = pop.drop_duplicates()
    
    mid_final = pd.merge(pop, mid, how='left', on=['mid'])
    mid_final = mid_final.sort_values(by=['mid'])
    mid_final = mid_final[mid_final['mid'].notna()]
    mid_final = mid_final.reset_index(drop=True)
    
    all_mid = mid_final.copy()
    # all_mid = all_mid[all_mid['mid'] == '000001006560000'] # sampling
    # all_mid = all_mid.sample(1000) # sampling
    all_mid = list(all_mid['mid'])
    len_mid = len(all_mid)
    
    hd = pd.read_csv('/project/data/thermal_paper_prediction/additional_info/holiday.csv', sep=';')
    hd['ds'] = pd.to_datetime(hd['ds'], format='ISO8601')
    
    global_trend = pd.read_csv('/project/data/thermal_paper_prediction/additional_info/global_trends/global_trend-{}{}-{}.csv'.format(sum_area_from_year_str, sum_area_from_month_str, global_param[:-6]), sep=';')
    global_trend['ds'] = pd.to_datetime(global_trend['ds'], format='ISO8601')
    
    #================================================================================================================================================#
    #================================================================ THE GRAND LOOP ================================================================#
    #================================================================================================================================================#
    
    push_log('Next Shawwal (Eid) will Start On {}'.format(shawwal_start))
    
    step_total = int(np.ceil(len(all_mid) / batch_size))
    push_log('Started Predicting Thermal Paper Usage. Train Date: LTE {}-{}-{}. Predict From: {}-{}-{}. Predict To: {}-{}-{}. Batch Size: {}. MID Total: {}. Step Total: {}. Step Start: {}'.format(max_train_year_str, max_train_month_str, max_train_day_str, sum_area_from_year_str, sum_area_from_month_str, sum_area_from_day_str, sum_area_to_year_str, sum_area_to_month_str, sum_area_to_day_str, batch_size, len(all_mid), step_total, step_start))
    
    sum_choice = [i for i in range(50, 76, 1)] # [50, 51, 52, ..., 75]
    # sum_choice = [50]
        
    start_time_step = time.time()
    start_time_total = time.time()
    step_count = step_start - 1
    
    if step_start == 1:
        init = batch_size
    else:
        init = step_start * batch_size  
    
    for step_start, step_end in zip(range(init - batch_size, len(all_mid), batch_size), range(init, len(all_mid) + batch_size, batch_size)):
        df_lst = []
        mid_step = all_mid[step_start:step_end]

        for mid in mid_step:
            query = {
                'query': {
                    'bool': {
                        'must': [
                            {
                                'term': {
                                    'mid': {
                                        'value': mid
                                    }
                                }
                            }
                        ]
                    }
                }
            }
            result = list(es_helper.scan(es, query=query, index=['transform-transaction_summary_thermal_paper_consumption*']))
            if len(result) == 0:
                push_log('No Record Found for MID {}. Continuing...'.format(mid))
                continue
            df_01_raw_elk = pd.json_normalize(result)
            df_01_raw_elk['_source.created_date'] = pd.to_datetime(df_01_raw_elk['_source.created_date'], format='ISO8601')
            df_01_raw_elk = df_01_raw_elk[df_01_raw_elk['_source.created_date'] <= max_train_date]
            df_01_raw_elk['day_count'] = 1

            df_tmp_append_max_date = pd.DataFrame()
            df_tmp_append_max_date['_source.created_date'] = ['{}-{}-{} 17:00:00'.format(append_max_year_str, append_max_month_str, append_max_day_str)] # max date to train data
            df_tmp_append_max_date['_source.mid'] = [mid]
            df_tmp_append_max_date['_source.number_of_transaction'] = 0.0
            df_tmp_append_max_date['_source.number_of_settlement'] = 0.0
            df_tmp_append_max_date['_source.number_of_generate_qr'] = 0.0
            # df_tmp_append_max_date['_source.paper_consumption'] = 0.0
            df_tmp_append_max_date['_source.created_date'] = pd.to_datetime(df_tmp_append_max_date['_source.created_date'], format='ISO8601')
            df_tmp_append_max_date['_source.created_date'] = df_tmp_append_max_date['_source.created_date'] + pd.Timedelta(hours=7)

            df_02_variable_count = df_01_raw_elk.copy()
            df_02_variable_count['_source.created_date'] = df_02_variable_count['_source.created_date'] + pd.Timedelta(hours=7)
            if list(df_tmp_append_max_date['_source.created_date'].dt.strftime('%Y-%m-%d'))[0] not in list(df_02_variable_count['_source.created_date'].dt.strftime('%Y-%m-%d')):
                df_02_variable_count = pd.concat([df_02_variable_count, df_tmp_append_max_date])
            df_02_variable_count = df_02_variable_count.sort_values(by=['_source.created_date'], ascending=True)
            df_02_variable_count = df_02_variable_count.reset_index(drop=True)
            
            if global_param == 'transaction_count':
                df_02_variable_count = df_02_variable_count.rename(columns={
                    '_source.mid': 'mid',
                    '_source.number_of_transaction': 'y',
                    '_source.number_of_settlement': 'settlement_count',
                    '_source.number_of_generate_qr': 'generate_qr_count',
                    # '_source.paper_consumption': 'paper_consumption',
                    '_source.created_date': 'ds'
                })
                df_02_variable_count = df_02_variable_count[['ds', 'mid', 'settlement_count', 'generate_qr_count', 'y', 'day_count']]
                df_03_inject_date = df_02_variable_count.copy()
                
                df_03_inject_date = df_03_inject_date.drop_duplicates(subset=['ds']) #remove duplicate ds
                
                df_03_inject_date.index = df_03_inject_date['ds']
                df_03_inject_date = df_03_inject_date.asfreq('D')
                df_03_inject_date = df_03_inject_date.drop(['ds'], axis=1)
                df_03_inject_date = df_03_inject_date.reset_index()
                df_03_inject_date['mid'] = df_03_inject_date['mid'].unique()[0]
                # df_03_inject_date['paper_consumption'] = df_03_inject_date['paper_consumption'].replace(np.nan, 0)
                df_03_inject_date['settlement_count'] = df_03_inject_date['settlement_count'].replace(np.nan, 0)
                df_03_inject_date['generate_qr_count'] = df_03_inject_date['generate_qr_count'].replace(np.nan, 0)
                df_03_inject_date['y'] = df_03_inject_date['y'].replace(np.nan, 0)
                
            elif global_param == 'settlement_count':
                df_02_variable_count = df_02_variable_count.rename(columns={
                    '_source.mid': 'mid',
                    '_source.number_of_transaction': 'transaction_count',
                    '_source.number_of_settlement': 'y',
                    '_source.number_of_generate_qr': 'generate_qr_count',
                    # '_source.paper_consumption': 'paper_consumption',
                    '_source.created_date': 'ds'
                })
                df_02_variable_count = df_02_variable_count[['ds', 'mid', 'y', 'generate_qr_count', 'transaction_count', 'day_count']]
                df_03_inject_date = df_02_variable_count.copy()
                
                df_03_inject_date = df_03_inject_date.drop_duplicates(subset=['ds']) #remove duplicate ds
                
                df_03_inject_date.index = df_03_inject_date['ds']
                df_03_inject_date = df_03_inject_date.asfreq('D')
                df_03_inject_date = df_03_inject_date.drop(['ds'], axis=1)
                df_03_inject_date = df_03_inject_date.reset_index()
                df_03_inject_date['mid'] = df_03_inject_date['mid'].unique()[0]
                # df_03_inject_date['paper_consumption'] = df_03_inject_date['paper_consumption'].replace(np.nan, 0)
                df_03_inject_date['transaction_count'] = df_03_inject_date['transaction_count'].replace(np.nan, 0)
                df_03_inject_date['generate_qr_count'] = df_03_inject_date['generate_qr_count'].replace(np.nan, 0)
                df_03_inject_date['y'] = df_03_inject_date['y'].replace(np.nan, 0)
                
            elif global_param == 'generate_qr_count':
                df_02_variable_count = df_02_variable_count.rename(columns={
                    '_source.mid': 'mid',
                    '_source.number_of_transaction': 'transaction_count',
                    '_source.number_of_settlement': 'settlement_count',
                    '_source.number_of_generate_qr': 'y',
                    # '_source.paper_consumption': 'paper_consumption',
                    '_source.created_date': 'ds'
                })
                df_02_variable_count = df_02_variable_count[['ds', 'mid', 'settlement_count', 'y', 'transaction_count', 'day_count']]
                df_03_inject_date = df_02_variable_count.copy()
                
                df_03_inject_date = df_03_inject_date.drop_duplicates(subset=['ds']) #remove duplicate ds
                
                df_03_inject_date.index = df_03_inject_date['ds']
                df_03_inject_date = df_03_inject_date.asfreq('D')
                df_03_inject_date = df_03_inject_date.drop(['ds'], axis=1)
                df_03_inject_date = df_03_inject_date.reset_index()
                df_03_inject_date['mid'] = df_03_inject_date['mid'].unique()[0]
                # df_03_inject_date['paper_consumption'] = df_03_inject_date['paper_consumption'].replace(np.nan, 0)
                df_03_inject_date['transaction_count'] = df_03_inject_date['transaction_count'].replace(np.nan, 0)
                df_03_inject_date['settlement_count'] = df_03_inject_date['settlement_count'].replace(np.nan, 0)
                df_03_inject_date['y'] = df_03_inject_date['y'].replace(np.nan, 0)

            df_04_filter_case = df_03_inject_date.copy()
            if df_04_filter_case['ds'].min() > max_train_date - pd.Timedelta(days=89): # exclude mids that have less than 90 days worth of transaction
                df_05_special_case = pd.DataFrame()
                df_05_special_case['mid'] = [mid]
                df_05_special_case['historical_sum_actual_all'] = df_03_inject_date['y'][df_03_inject_date['ds'] <= max_train_date].sum()
                df_05_special_case['historical_sum_actual_last_one_month'] = df_03_inject_date['y'][(df_03_inject_date['ds'] >= last_month_date) & (df_03_inject_date['ds'] <= max_train_date)].sum()
                df_05_special_case['historical_day_first_log'] = df_04_filter_case['ds'][df_04_filter_case['ds'] <= max_train_date].min()
                df_05_special_case['historical_day_last_log'] = df_01_raw_elk['_source.created_date'][df_01_raw_elk['_source.created_date'] <= max_train_date].max()
                df_05_special_case['historical_day_active'] = df_02_variable_count['day_count'][df_02_variable_count['ds'] <= max_train_date].sum()

                df_05_special_case['historical_day_first_log'] = pd.to_datetime(df_05_special_case['historical_day_first_log'], format=('%Y-%m-%d'))
                df_05_special_case['historical_day_last_log'] = pd.to_datetime(df_05_special_case['historical_day_last_log'], format=('%Y-%m-%d %H:%M:%S'))
                df_05_special_case['historical_day_last_log'] = df_05_special_case['historical_day_last_log'] + pd.Timedelta(hours=7)

                adjusted_roll_diff_days = (max_train_date - df_04_filter_case['ds'].min()).days + 1
                if adjusted_roll_diff_days < 1:
                    adjusted_roll_diff_days = 1
                df_05_special_case['sum_actual_special'] = df_05_special_case['historical_sum_actual_all'] * (30 / adjusted_roll_diff_days) # prorate untuk 30 hari ke depan

                for i in sum_choice:
                    df_05_special_case['sum_{}'.format(i)] = df_05_special_case['sum_actual_special']
                df_05_special_case['tag'] = ['special_case']

                df_05_special_case = df_05_special_case.drop(['sum_actual_special'], axis=1)

                df_lst.append(df_05_special_case)

            else: 
                df_05_prepare_forecast = df_04_filter_case.copy()
                df_05_prepare_forecast = df_05_prepare_forecast[['ds', 'y']]
                df_05_prepare_forecast = df_05_prepare_forecast[df_05_prepare_forecast['ds'] <= max_train_date] # train data

                model = Prophet(holidays=hd)
                model.fit(df_05_prepare_forecast)
                future = model.make_future_dataframe(periods=predict_x_days)
                df_06_predicted = model.predict(future)
                df_06_predicted = df_06_predicted.reset_index(drop=True)
                # df_06_predicted = df_06_predicted[['ds', 'yhat', 'yhat_upper', 'yhat_lower']]
                # if df_06_predicted['holidays'].sum() == 0:
                #     df_06_predicted = pd.concat([df_06_predicted.loc[(df_06_predicted['ds'] < sum_area_from_date)], global_trend.loc[(global_trend['ds'] >= sum_area_from_date)]])

                df_07_calculate_sum_area = df_06_predicted.copy()
                df_07_calculate_sum_area['mid'] = df_04_filter_case['mid'].unique()[0]
                df_07_calculate_sum_area = df_07_calculate_sum_area[(df_07_calculate_sum_area['ds'] >= sum_area_from_date) & (df_07_calculate_sum_area['ds'] <= sum_area_to_date)] # forecast result
                df_07_calculate_sum_area = df_07_calculate_sum_area[['mid', 'ds', 'yhat', 'yhat_upper', 'yhat_lower']]
                for i in sum_choice:
                    df_07_calculate_sum_area[i] = 0
                    if i <= 50:
                        df_07_calculate_sum_area.loc[:, i] = df_07_calculate_sum_area['yhat_lower'] + (df_07_calculate_sum_area['yhat'] - df_07_calculate_sum_area['yhat_lower']) * i/50
                    elif i > 50:
                        df_07_calculate_sum_area.loc[:, i] = df_07_calculate_sum_area['yhat'] + (df_07_calculate_sum_area['yhat_upper'] - df_07_calculate_sum_area['yhat']) * (i-50)/50
                df_07_calculate_sum_area['actual'] = df_03_inject_date['y']

                df_08 = df_07_calculate_sum_area.copy()

                df_09_sum_area = pd.DataFrame()
                df_09_sum_area['mid'] = [df_04_filter_case['mid'].unique()[0]]
                df_09_sum_area['tag'] = ['normal_case']

                for j in sum_choice:
                    df_09_sum_area['sum_{}'.format(j)] = [df_08[j].sum()]
                df_09_sum_area['historical_sum_actual_all'] = df_03_inject_date['y'][df_03_inject_date['ds'] <= max_train_date].sum() # get actual sum
                df_09_sum_area['historical_sum_actual_last_one_month'] = df_03_inject_date['y'][(df_03_inject_date['ds'] >= last_month_date) & (df_03_inject_date['ds'] <= max_train_date)].sum()
                df_09_sum_area['historical_day_first_log'] = df_04_filter_case['ds'][df_04_filter_case['ds'] <= max_train_date].min()
                df_09_sum_area['historical_day_last_log'] = df_01_raw_elk['_source.created_date'][df_01_raw_elk['_source.created_date'] <= max_train_date].max()
                df_09_sum_area['historical_day_active'] = df_02_variable_count['day_count'][df_02_variable_count['ds'] <= max_train_date].sum()

                df_09_sum_area['historical_day_first_log'] = pd.to_datetime(df_09_sum_area['historical_day_first_log'], format=('%Y-%m-%d'))
                df_09_sum_area['historical_day_last_log'] = pd.to_datetime(df_09_sum_area['historical_day_last_log'], format=('%Y-%m-%d %H:%M:%S'))
                df_09_sum_area['historical_day_last_log'] = df_09_sum_area['historical_day_last_log'] + pd.Timedelta(hours=7)

                df_lst.append(df_09_sum_area)

        df_steps = pd.concat(df_lst)
        df_steps = df_steps.reset_index(drop=True)
        
        step_count += 1
        step_str = str(step_count)
        
        from_date = str(max_train_year) + ('0' + str(max_train_month) if len(str(max_train_month)) == 1 else str(max_train_month)) + ('0' + str(max_train_day) if len(str(max_train_day)) == 1 else str(max_train_day))
        to_date = (max_train_date + date.timedelta(days=predict_x_days)).strftime('%Y%m%d')
        df_steps.to_csv('/project/data/thermal_paper_prediction/{}/steps_batch_size_{}_param_{}/step_{}_of_{}.csv'
                        .format(dir_name,
                                batch_size,
                                global_param,
                                step_str,
                                step_total,
                               ), sep=';', index=False
                       )
        push_log('Step {}/{}. Step Run Time:{:.3f}s. Total Run Time:{:.3f}s'.format(step_str, step_total, time.time()-start_time_step, time.time()-start_time_total))
        start_time_step = time.time()  
    push_log('Finished Predicting')
    steps_dir_name = '{}/steps_batch_size_{}_param_{}'.format(dir_name, batch_size, global_param)
    
    push_log('Started Concatenating All DataFrame Steps, Removing All Files from Directory {} and Saving the Data Into Directory ready_to_merge_with_population'.format(steps_dir_name))
    df_final = concat_dfs(path='/project/data/thermal_paper_prediction/{}/'.format(steps_dir_name))
    df_final['dir_name'] = steps_dir_name
    df_final['max_train_date'] = max_train_date
    rmdir('/project/data/thermal_paper_prediction/{}/{}/'.format(steps_dir_name.split('/')[0], steps_dir_name.split('/')[1]))
    
    df_final.to_csv('/project/data/thermal_paper_prediction/ready_to_merge_with_population/raw_{}.csv'.format(global_param), sep=';', index=False)
    df_final.to_csv('/project/data/thermal_paper_prediction/{}/raw_{}_{}.csv'.format(steps_dir_name.split('/')[0], global_param, steps_dir_name.split('/')[0]), sep=';', index=False)
    df_final.to_excel('/project/data/thermal_paper_prediction/{}/raw_{}_{}.xlsx'.format(steps_dir_name.split('/')[0], global_param, steps_dir_name.split('/')[0]), index=False)
    push_log('Finished Concatenating All DataFrame Steps, Removing All Files from Directory {} and Saving the Data Into Directory ready_to_merge_with_population'.format(steps_dir_name))
    
    push_log('Finished Grand Loop for {}'.format(global_param))


def transform_grand_loop(sum_choice=50, one_roll_total_cm=1150, cm_trx_with_brimo=14.7, cm_trx_with_no_brimo=10, cm_stl=36, cm_gqr=13.5, default_pm_needs_rolls=1, default_print_behavior=3, default_reprint_behavior=0, max_of_reprint_behavior=0.5):
    """
    """
    push_log("Started Getting Population")
    pop = pd.read_csv("/project/data/thermal_paper_prediction/mid_and_pop/pop.csv", sep=";", dtype="str")
    pop_01_sorted = pop[["mid", "tid", "poi", "serial_number", "merchant_name", "merchant_criteria", "store_name", "store_address", "provinsi", "kab_kot", "kanwil", "installed_date", "ticket_latest_team"]].rename(columns={"ticket_latest_team": "team"})
    pop_01_sorted["installed_date"] = pd.to_datetime(pop_01_sorted["installed_date"])
    
    tmp_pop_01_sorted = pop_01_sorted.copy()
    tmp_pop_01_sorted = tmp_pop_01_sorted[["mid"]]
    tmp_pop_01_sorted["tid_count"] = 1
    tmp_pop_01_sorted = tmp_pop_01_sorted.groupby(by=["mid"]).count()
    tmp_pop_01_sorted = tmp_pop_01_sorted.reset_index()
    
    pop_02_tid_count = pd.merge(pop_01_sorted, tmp_pop_01_sorted, how="left", on=["mid"])
    push_log("Finished Getting Population")
    
    push_log("Started Reading Raw Data")
    df_trx = pd.read_csv("/project/data/thermal_paper_prediction/ready_to_merge_with_population/raw_transaction_count.csv", sep=";", dtype={"mid": "str"})
    df_stl = pd.read_csv("/project/data/thermal_paper_prediction/ready_to_merge_with_population/raw_settlement_count.csv", sep=";", dtype={"mid": "str"})
    df_gqr = pd.read_csv("/project/data/thermal_paper_prediction/ready_to_merge_with_population/raw_generate_qr_count.csv", sep=";", dtype={"mid": "str"})
    steps_dir_name = df_trx["dir_name"].unique()[0].split("/")[0]
    # steps_dir_name = "thermal_paper_lte_20250301_sum_area_20250401_to_20250430"
    max_train_date = date.datetime.strptime(df_trx["max_train_date"].unique()[0], "%Y-%m-%d")
    push_log("Finished Reading Raw Data")
    
    push_log('Started Transforming Data')
    df_trx = df_trx[["mid", f"sum_{sum_choice}"]].rename(columns={f"sum_{sum_choice}": "forecast_transaction"})
    df_trx.loc[df_trx["forecast_transaction"] < 0, "forecast_transaction"] = 0.0
    df_trx["forecast_transaction"] = np.ceil(df_trx["forecast_transaction"])
    df_trx = df_trx.drop_duplicates()
    df_stl = df_stl[["mid", f"sum_{sum_choice}"]].rename(columns={f"sum_{sum_choice}": "forecast_settlement"})
    df_stl.loc[df_stl["forecast_settlement"] < 0, "forecast_settlement"] = 0.0
    df_stl["forecast_settlement"] = np.ceil(df_stl["forecast_settlement"])
    df_stl = df_stl.drop_duplicates()
    df_gqr = df_gqr[["mid", f"sum_{sum_choice}"]].rename(columns={f"sum_{sum_choice}": "forecast_generate_qr"})
    df_gqr.loc[df_gqr["forecast_generate_qr"] < 0, "forecast_generate_qr"] = 0.0
    df_gqr["forecast_generate_qr"] = np.ceil(df_gqr["forecast_generate_qr"])
    df_gqr = df_gqr.drop_duplicates()
    
    df_10_init_1 = pd.merge(df_trx, df_stl, how="left", on=["mid"])
    df_10_init_2 = pd.merge(df_10_init_1, df_gqr, how="left", on=["mid"])
    df_10_init = pd.merge(pop_02_tid_count, df_10_init_2, how="left", on=["mid"])
    df_10_init.loc[df_10_init["forecast_transaction"].isna(), "forecast_transaction"] = 1.0
    df_10_init.loc[df_10_init["forecast_settlement"].isna(), "forecast_settlement"] = 1.0
    df_10_init.loc[df_10_init["forecast_generate_qr"].isna(), "forecast_generate_qr"] = 1.0

    bvr = pd.read_csv("/project/data/thermal_paper_prediction/additional_info/mid_behavior_aggregated.csv", sep=";", dtype={"mid": "str", "qr_is_included": "str"})
    bvr = bvr.drop(["record_count", "number_of_settlement", "number_of_generate_qr"], axis=1)
    bvr.loc[bvr["qr_is_included"] == "1", "qr_is_included"] = True
    bvr.loc[bvr["qr_is_included"] == "0", "qr_is_included"] = False

    df_11_behavior = pd.merge(df_10_init, bvr, how="left", on=["mid"])
    df_11_behavior["print_behavior"] = df_11_behavior["sum_of_print_count"] / df_11_behavior["number_of_print_count"]
    df_11_behavior["print_behavior"] = np.ceil(df_11_behavior["print_behavior"] * 2) / 2
    df_11_behavior.loc[(df_11_behavior["sum_of_print_count"] == 0) & (df_11_behavior["number_of_print_count"] == 0), "print_behavior"] = default_print_behavior
    df_11_behavior["reprint_behavior"] = (df_11_behavior["number_of_reprint"] / df_11_behavior["number_of_transaction"])
    df_11_behavior.loc[(df_11_behavior["number_of_transaction"] == 0) & (df_11_behavior["print_behavior"].notna()) & (df_11_behavior["reprint_behavior"].isna()), "reprint_behavior"] = default_reprint_behavior
    df_11_behavior.loc[df_11_behavior["qr_is_included"].isna(), "qr_is_included"] = True
    df_11_behavior = df_11_behavior.drop(["sum_of_print_count", "number_of_print_count", "number_of_reprint", "number_of_transaction"], axis=1)
    df_11_behavior.loc[df_11_behavior["print_behavior"].isna(), "print_behavior"] = default_print_behavior
    df_11_behavior.loc[df_11_behavior["reprint_behavior"].isna(), "reprint_behavior"] = default_reprint_behavior
    df_11_behavior.loc[np.isinf(df_11_behavior["reprint_behavior"]), "reprint_behavior"] = default_reprint_behavior
    
    df_12_divide_by_tid = df_11_behavior.copy()
    df_12_divide_by_tid["forecast_transaction"] = np.ceil(df_12_divide_by_tid["forecast_transaction"] / df_12_divide_by_tid["tid_count"])
    df_12_divide_by_tid["forecast_settlement"] = np.ceil(df_12_divide_by_tid["forecast_settlement"] / df_12_divide_by_tid["tid_count"])
    df_12_divide_by_tid["forecast_generate_qr"] = np.ceil(df_12_divide_by_tid["forecast_generate_qr"] / df_12_divide_by_tid["tid_count"])
    
    df_13_effect = df_12_divide_by_tid.copy()
    df_13_effect["transaction_brimo_qr_included_cm"] = np.ceil(df_13_effect["forecast_transaction"] * cm_trx_with_brimo)
    df_13_effect.loc[df_13_effect["qr_is_included"] == False, "transaction_brimo_qr_included_cm"] = np.ceil(df_13_effect["forecast_transaction"] * cm_trx_with_no_brimo)
    df_13_effect["transaction_print_effect_cm"] = np.ceil(df_13_effect["print_behavior"] * df_13_effect["transaction_brimo_qr_included_cm"])
    def reprint_effect(x):
        if x["qr_is_included"] == True:
            if x["reprint_behavior"] > max_of_reprint_behavior:
                result = np.ceil(np.ceil(x["forecast_transaction"] * max_of_reprint_behavior) * cm_trx_with_brimo)
                return result
            result = np.ceil(np.ceil(x["forecast_transaction"] * x["reprint_behavior"]) * cm_trx_with_brimo)
            return result
        else:
            if x["reprint_behavior"] > max_of_reprint_behavior:
                result = np.ceil(np.ceil(x["forecast_transaction"] * max_of_reprint_behavior) * cm_trx_with_no_brimo)
                return result
            result = np.ceil(np.ceil(x["forecast_transaction"] * x["reprint_behavior"]) * cm_trx_with_no_brimo)
            return result
    df_13_effect["transaction_reprint_effect_cm"] = df_13_effect.apply(lambda x: reprint_effect(x), axis=1)
    
    df_14_sum = df_13_effect.copy()
    # df_14_sum["transaction_cm"] = df_14_sum["transaction_brimo_qr_included_cm"] + df_14_sum["transaction_print_effect_cm"] + df_14_sum["transaction_reprint_effect_cm"]
    df_14_sum["transaction_cm"] = df_14_sum["transaction_print_effect_cm"] + df_14_sum["transaction_reprint_effect_cm"]
    df_14_sum["settlement_cm"] = np.ceil(df_14_sum["forecast_settlement"] * cm_stl)
    df_14_sum["generate_qr_cm"] = np.ceil(df_14_sum["forecast_generate_qr"] * cm_gqr)
    
    df_15_pred = df_14_sum.copy()
    df_15_pred["prediction_cm"] = df_15_pred["transaction_cm"] + df_15_pred["settlement_cm"] + df_15_pred["generate_qr_cm"]
    df_15_pred["prediction_rolls_before"] = np.ceil(df_15_pred["prediction_cm"] / one_roll_total_cm)
    df_15_pred["pm_needs_rolls"] = df_15_pred["prediction_rolls_before"]
    df_15_pred.loc[(df_15_pred["prediction_rolls_before"] == 0) | (df_15_pred["prediction_rolls_before"].isna()), "pm_needs_rolls"] = default_pm_needs_rolls
    df_15_pred.loc[df_15_pred["pm_needs_rolls"] >= 100, "pm_needs_rolls"] = np.ceil(df_15_pred["pm_needs_rolls"] / 5) * 5
    df_15_pred["prediction_rolls"] = df_15_pred["pm_needs_rolls"]
    
    df_16_add_cols = df_15_pred.copy()
    df_16_add_cols["mark"] = df_16_add_cols["mark"] = steps_dir_name.replace("_", "")[-36:]
    df_16_add_cols["sum_choice"] = sum_choice
    df_16_add_cols["one_roll_total_cm"] = one_roll_total_cm
    df_16_add_cols["created_at"] = date.datetime.now() + pd.Timedelta(hours=7)
    df_16_add_cols["created_at"] = df_16_add_cols["created_at"].astype("str")
    df_16_add_cols["created_at"] = df_16_add_cols["created_at"].str[:-3]
    df_16_add_cols["created_at"] = pd.to_datetime(df_16_add_cols["created_at"], format=("%Y-%m-%d %H:%M:%S.%f"))
    df_16_add_cols["updated_at"] = df_16_add_cols["created_at"]
    df_16_add_cols["prediction_for_year"] = steps_dir_name[-20:-16]
    df_16_add_cols["prediction_for_month"] = steps_dir_name[-16:-14]
    df_16_add_cols["prediction_for_month"] = df_16_add_cols["prediction_for_month"].astype("int").astype("str")
    
    df_17_tidyup = df_16_add_cols.copy()
    for i in [col for col in df_17_tidyup.select_dtypes(include="object").columns if col != "qr_is_included"]:
        df_17_tidyup.loc[df_17_tidyup[i].isna(), i] = "-"
    for i in [col for col in df_17_tidyup.select_dtypes(include="float").columns if col != "print_behavior"]:
        df_17_tidyup.loc[df_17_tidyup[i].isna(), i] = 0
    for i in [col for col in df_17_tidyup.select_dtypes(include="datetime").columns]:
        df_17_tidyup.loc[df_17_tidyup[i].isna(), i] = date.datetime(2000, 1, 1, 2, 2, 2, 123)
    df_17_tidyup.loc[df_17_tidyup["qr_is_included"].isna(), "qr_is_included"] = True
    df_17_tidyup.loc[df_17_tidyup["print_behavior"].isna(), "print_behavior"] = default_print_behavior
    for i in [col for col in df_17_tidyup.select_dtypes(include="float").columns if col not in ["print_behavior", "reprint_behavior"]]:
        df_17_tidyup[i] = df_17_tidyup[i].astype("int")
    
    df_18_add_cols = df_17_tidyup.copy()
    df_18_add_cols["cm_transaction_with_brimo"] = cm_trx_with_brimo
    df_18_add_cols["cm_transaction_with_no_brimo"] = cm_trx_with_no_brimo
    df_18_add_cols["cm_generate_qr"] = cm_gqr
    df_18_add_cols["cm_settlement"] = cm_stl
    df_18_add_cols["default_print_behavior"] = default_print_behavior
    df_18_add_cols["default_reprint_behavior"] = default_reprint_behavior
    df_18_add_cols["default_pm_needs_rolls"] = default_pm_needs_rolls
        
    df_19_sort_fields = df_18_add_cols.copy()
    df_19_sort_fields = df_19_sort_fields[[
        "mid",
        "poi",
        "tid",
        "serial_number",
        "merchant_name",
        "merchant_criteria",
        "store_name",
        "store_address",
        "provinsi",
        "kab_kot",
        "kanwil",
        "installed_date",
        "team",
        "tid_count",
        "forecast_transaction",
        "forecast_generate_qr",
        "forecast_settlement",
        "print_behavior",
        "reprint_behavior",
        "qr_is_included",
        "transaction_brimo_qr_included_cm",
        "transaction_print_effect_cm",
        "transaction_reprint_effect_cm",
        "transaction_cm",
        "generate_qr_cm",
        "settlement_cm",
        "prediction_cm",
        "prediction_rolls_before",
        "pm_needs_rolls",
        "prediction_rolls",
        "prediction_for_year",
        "prediction_for_month",
        "mark",
        "sum_choice",
        "one_roll_total_cm",
        "cm_transaction_with_brimo",
        "cm_transaction_with_no_brimo",
        "cm_generate_qr",
        "cm_settlement",
        "default_print_behavior",
        "default_reprint_behavior",
        "default_pm_needs_rolls",
        "created_at",
        "updated_at"
    ]]
    
    df_transformed = df_19_sort_fields.copy()
    push_log("Finished Transforming Data")

    push_log(f"Started Saving the Transformed Data to Directory {steps_dir_name}")
    df_transformed.to_csv(f"/project/data/thermal_paper_prediction/{steps_dir_name}/transformed_{steps_dir_name}.csv", sep=";", index=False)
    # df_transformed.to_excel(f"/project/data/thermal_paper_prediction/{steps_dir_name}/transformed_{steps_dir_name}.xlsx", index=False)
    push_log(f"Finished Saving the Transformed Data to Directory {steps_dir_name}")
    
    push_log(f"Started Saving the Data Into Directory ready_to_insert_to_elk")
    df_transformed.to_csv("/project/data/thermal_paper_prediction/adjusting_roll/transformed.csv", sep=";", index=False)
    push_log("Finished Saving the Data Into Directory ready_to_insert_to_elk")

def get_roll_adj_by_tid():
    '''
    Get adjusted average roll per tid from ELK
    '''
    push_log('Started Getting Roll Adjustment per TID')

    es = connect_elasticsearch_read()

    query = {
        "runtime_mappings": {
            "roll_adj": {
                "type": "long",
                "script": {
                    "source": """
                        if (params._source.containsKey('tag_name') && params._source.tag_name != null) {
                            def value = params._source.tag_name;
                            def m = /\\d+/.matcher(value);
                            if (m.find()) {
                                emit(Long.parseLong(m.group()));
                            }
                        }
                    """
                }
            }
        },
        "query": {
            "bool": {
                "filter": [
                    {"term": {"kategori_name.keyword": "Support Maintenance"}},
                    {"term": {"parent_tag_name.keyword": "THERMAL PAPER"}},
                    {
                        "range": {
                            "date_created_sub_tiket": {
                                "gte": "now-2M/M",
                                "lt": "now/M",
                                "time_zone": "+07:00"
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "by_tid": {
                "terms": {
                    "field": "tid.keyword",
                    "size": 1000
                },
                "aggs": {
                    "sum_roll": {
                        "sum": {
                            "field": "roll_adj"
                        }
                    },
                    "adjusted_avg": {
                        "bucket_script": {
                            "buckets_path": {
                                "sumValue": "sum_roll"
                            },
                            "script": "Math.ceil(params.sumValue / 2)"
                        }
                    }
                }
            }
        },
        "size": 0
    }

    response = es.search(
        index=[
            "logstash-ticket_implementation-v02",
            "logstash-ticket_maintenance-v02*"
        ],
        body=query
    )

    buckets = response.get('aggregations', {}).get('by_tid', {}).get('buckets', [])

    print("DEBUG BUCKET SAMPLE:")
    if buckets:
        print(buckets[0])
    else:
        print("No buckets found")

    data = []
    for bucket in buckets:

        adjusted_avg = None
        if bucket.get('adjusted_avg') is not None:
            adjusted_avg = bucket['adjusted_avg'].get('value')

        data.append({
            'tid': bucket.get('key'),
            'doc_count': bucket.get('doc_count'),
            'sum_roll': bucket.get('sum_roll', {}).get('value'),
            'adjusted_avg': adjusted_avg
        })

    df_adj = pd.DataFrame(data)

    if not df_adj.empty:
        df_adj = df_adj.sort_values(by=['tid']).reset_index(drop=True)
        df_adj['adjusted_avg'] = df_adj['adjusted_avg'].fillna(0).astype(int)

    df_adj.to_csv(
        '/project/data/thermal_paper_prediction/adjusting_roll/sm_adjust.csv',
        sep=';',
        index=False
    )

    push_log('Finished Getting Roll Adjustment per TID')

    # return df_adj

def adjust_roll_prediction(
    path_adjusting="/project/data/thermal_paper_prediction/adjusting_roll/sm_adjust.csv",
    path_transformed="/project/data/thermal_paper_prediction/adjusting_roll/transformed.csv"
):

    # read files
    df_adj = pd.read_csv(path_adjusting, sep=";")
    df_transformed = pd.read_csv(path_transformed, sep=";")

    # ambil kolom
    df_adj = df_adj[['tid', 'adjusted_avg']]

    # samakan tipe
    df_transformed['tid'] = df_transformed['tid'].astype(str)
    df_adj['tid'] = df_adj['tid'].astype(str)

    # merge
    df = df_transformed.merge(df_adj, on='tid', how='left')

    mask = df['adjusted_avg'].notna()

    # adjustment
    df.loc[mask, 'prediction_rolls'] = (
        df.loc[mask, 'prediction_rolls'] + df.loc[mask, 'adjusted_avg']
    )

    df.loc[mask, 'pm_needs_rolls'] = (
        df.loc[mask, 'pm_needs_rolls'] + df.loc[mask, 'adjusted_avg']
    )

    df = df.drop(columns=['adjusted_avg'])

    print("Total TID adjusted:", mask.sum())

    # simpan kembali
    df.to_csv('/project/data/thermal_paper_prediction/ready_to_insert_to_elk/transformed_with_adjusting.csv',sep=";", index=False)

    # return df

    
def populate_index(index_name, path, insert_batch_size=10, insert_step_start=1):
    '''
    '''
    es = connect_elasticsearch_write()
    
    df = pd.read_csv(path, sep=';', dtype={'mid': 'str'})
    # df = pd.read_csv(path, sep=',', dtype={'mid': 'str'})
    df = df[df['installed_date'].notna()]
    df = df.replace(np.nan, '-')
    
    if path == '/project/data/thermal_paper_prediction/ready_to_merge_with_population/raw.csv':
        df['historical_day_first_log'] = df['historical_day_first_log'].replace(np.nan, df['max_train_date'].unique()[0])
        df['historical_day_first_log'] = pd.to_datetime(df['historical_day_first_log'], format='ISO8601')
        df['historical_day_first_log'] = df['historical_day_first_log'] - pd.Timedelta(hours=7)
        df['historical_day_first_log'] = df['historical_day_first_log'].astype(str)
        
        df['historical_day_last_log'] = df['historical_day_last_log'].replace(np.nan, df['max_train_date'].unique()[0])
        df['historical_day_last_log'] = pd.to_datetime(df['historical_day_last_log'], format='ISO8601')
        df['historical_day_last_log'] = df['historical_day_last_log'] - pd.Timedelta(hours=7)
        df['historical_day_last_log'] = df['historical_day_last_log'].astype(str)
        
        df['max_train_date'] = pd.to_datetime(df['max_train_date'], format='ISO8601')
        df['max_train_date'] = df['max_train_date'] - pd.Timedelta(hours=7)
        df['max_train_date'] = df['max_train_date'].astype(str)
        
    elif path == '/project/data/thermal_paper_prediction/ready_to_insert_to_elk/transformed_with_adjusting.csv':
        df['installed_date'] = pd.to_datetime(df['installed_date'], format='ISO8601')
        df['installed_date'] = df['installed_date'] - pd.Timedelta(hours=7)
        df['installed_date'] = df['installed_date'].astype(str)
        
        df['created_at'] = pd.to_datetime(df['created_at'], format='ISO8601')
        df['created_at'] = df['created_at'] - pd.Timedelta(hours=7)
        df['created_at'] = df['created_at'].astype(str)
        
        df['updated_at'] = pd.to_datetime(df['updated_at'], format='ISO8601')
        df['updated_at'] = df['updated_at'] - pd.Timedelta(hours=7)
        df['updated_at'] = df['updated_at'].astype(str)
        
    elif path == '/project/data/thermal_paper_prediction/ready_to_insert_to_elk/log.csv':
        df['created_at'] = pd.to_datetime(df['created_at'], format='ISO8601')
        df['created_at'] = df['created_at'] - pd.Timedelta(hours=7)
        df['created_at'] = df['created_at'].astype(str)
        # df['created_at'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
    
    push_log('Started Inserting {} Documents to Elasticsearch Index {}. Batch Size: {}'.format(len(df.index), index_name, insert_batch_size))
    
    start_time_step = time.time()
    start_time_total = time.time()
    
    insert_step_total = int(np.ceil(len(df) / insert_batch_size))
    insert_step_count = insert_step_start - 1
    
    
    if insert_step_start == 1:
        init = insert_batch_size
    else:
        init = insert_step_start * insert_batch_size
    
    for step_start, step_end in zip(range(init - insert_batch_size, len(df), insert_batch_size), range(init, len(df) + insert_batch_size, insert_batch_size)):
        df_step = df[step_start:step_end]
        
        # push_log('Insert Step {}/{}. Insert Step Run Time:{:.3f}s. Insert Total Run Time:{:.3f}s'.format(insert_step_count, insert_step_total, time.time()-start_time_step, time.time()-start_time_total))
        for doc in df_step.apply(lambda x: x.to_dict(), axis=1):
            es.index(index=index_name, body=json.dumps(doc))
        
        insert_step_count += 1
        if len(str(insert_step_count)) == 1:
            insert_step_str = '000' + str(insert_step_count)
        elif len(str(insert_step_count)) == 2:
            insert_step_str = '00' + str(insert_step_count)
        elif len(str(insert_step_count)) == 3:
            insert_step_str = '0' + str(insert_step_count)
        else:
            insert_step_str = str(insert_step_count)
        
        # if insert_step_count == 1:
        #     push_log('Starting Step {}/{}. Step Run Time:{:.3f}s. Total Run Time:{:.3f}s'.format(insert_step_str, insert_step_total, time.time()-start_time_step, time.time()-start_time_total))
        # else:
        #     push_log('Starting Step {}/{}. Previous Step Run Time:{:.3f}s. Total Run Time:{:.3f}s'.format(insert_step_str, insert_step_total, time.time()-start_time_step, time.time()-start_time_total))
        # push_log('Insert Step {}/{}. Insert Step Run Time:{:.3f}s. Insert Total Run Time:{:.3f}s'.format(insert_step_str, insert_step_total, time.time()-start_time_step, time.time()-start_time_total))
        start_time_step = time.time()
        # push_log('Sleep for 0.01 Seconds, Letting Elasticsearch Indexing...')
        time.sleep(1/10)
    
    push_log('Finished Inserting {} Documents to Elasticsearch Index {}'.format(len(df.index), index_name))

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--specify_path",
        default=None,
        help="Path output lama untuk resume"
    )
    parser.add_argument(
        "--step_start",
        type=int,
        default=1,
        help="Mulai dari step ke berapa"
    )

    args = parser.parse_args()

    log_timestamp = []
    log_message = []
    global_dir_name = []
    
    # get_mid()
    
    # get_population()
    
    # get_aggregated()
    
    # get_holiday_dates()
    
#     get_global_trend(global_param="generate_qr_count")

#     grand_loop(
#         global_param="generate_qr_count",
#         specify_path=args.specify_path,
#         step_start=args.step_start
#     )

# # grand_loop(global_param="generate_qr_count", step_start=1, specify_path="thermal_paper_lte_20251218_sum_area_20260101_to_20260131")

    transform_grand_loop()

    get_roll_adj_by_tid()

    adjust_roll_prediction()
    
    # populate_index(index_name="python-thermal_paper_integrate_transformed", path="/project/data/thermal_paper_prediction/ready_to_insert_to_elk/transformed_with_adjusting.csv", insert_step_start=1)