# v1.2.0
from elasticsearch import Elasticsearch
import elasticsearch.helpers as es_helper
import pandas as pd
import numpy as np
import warnings
import requests
import json
import os
import datetime as date
import pytz
import time
warnings.filterwarnings('ignore')


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
        
        
def connect_elasticsearch():
    '''
    username: string
    password: string
    '''
    es = Elasticsearch(
        ["https://m3sbridata-api.pcsindonesia.co.id:443"],
        api_key="T3dNUnQ1VUI1RWhvcjE2VWJfdm86SkRPaDl1bjBRNW1sa2lTUGNiS2U0dw==",
        verify_certs=False
    )
    
    # es = Elasticsearch(
    #         ['https://10.184.20.22:9200','https://10.184.20.24:9200','https://10.184.20.26:9200'],
    #         api_key='RklROTlaSUJhOHNtQ0hRM01pM0o6clRYTFJ6a0JSd0NBMDN1Q1dJVHRqdw==',
    #         verify_certs=False
    #     )
    
    return es

def send_webhook_notification(webhook_url, card):
    headers = {'Content-Type': 'application/json; charset=UTF-8'}

    payload = {
        "cards": [card]
    }

    response = requests.post(
        webhook_url,
        data=json.dumps(payload),
        headers=headers
    )
    
    # Check the response status
    if response.status_code == 200:
        print("Notification sent successfully!")
    else:
        print(f"Failed to send notification. Status code: {response.status_code}, Response: {response.text}")
        
        
def card_summary(first_time, last_time, duration, timeout_unique_tid, success_timeout_tid, bri_issue_tid, timeout_feature, bri_feature):
    card = {
        "header": {
            "title": "✅ Transaction Timeout Resolved",
        },
        "sections": [
            {
                "widgets": [
                    {
                        "keyValue": {
                            "topLabel": "Total Unique Timeout TID",
                            "content": str(timeout_unique_tid)
                        }
                    },
                    {
                        "keyValue": {
                            "topLabel": "Total Unique Success TID",
                            "content": str(success_timeout_tid)
                        }
                    },
                    {
                        "textParagraph": {
                            "text": "Top Timeout Features:\n" + str(timeout_feature)
                        }
                    }
                    
                ]
            },
             {
                "widgets": [
                    {
                        "keyValue": {
                            "topLabel": "Total unique TID on BRI Issue (Q1, Q2, Q3, Q4)",
                            "content": str(bri_issue_tid)
                        }
                    },
                    {
                        "textParagraph": {
                            "text": "Top BRI Features:\n" + str(bri_feature)
                        }
                    }
                ]
            },
             {
                "widgets": [
                    {
                        "keyValue": {
                            "topLabel": "Duration",
                            "content": f"{str(duration)} minute"
                        }
                    },
                    {
                        "keyValue": {
                            "topLabel": "Timeout Time",
                            "content": f"{str(first_time)} to {str(last_time)}"
                        }
                    }
                ]
            }
        ]
    }
    return card


def card_event(first_time, last_time, duration, timeout_unique_tid, success_timeout_tid, bri_issue_tid, timeout_feature, bri_feature):
    card = {
        "header": {
            "title": "🚨 Transaction Timeout Alert 🚨",
        },
        "sections": [
            {
                "widgets": [
                    {
                        "keyValue": {
                            "topLabel": "Total Unique Timeout TID",
                            "content": str(timeout_unique_tid)
                        }
                    },
                    {
                        "keyValue": {
                            "topLabel": "Total Unique Success TID",
                            "content": str(success_timeout_tid)
                        }
                    },
                    {
                        "textParagraph": {
                            "text": "Top Timeout Features:\n" + str(timeout_feature)
                        }
                    }
                    
                ]
            },
             {
                "widgets": [
                    {
                        "keyValue": {
                            "topLabel": "Total unique TID on BRI Issue (Q1, Q2, Q3, Q4)",
                            "content": str(bri_issue_tid)
                        }
                    },
                    {
                        "textParagraph": {
                            "text": "Top BRI Features:\n" + str(bri_feature)
                        }
                    }
                ]
            },
             {
                "widgets": [
                    {
                        "keyValue": {
                            "topLabel": "Duration",
                            "content": f"{str(duration)} minute"
                        }
                    },
                    {
                        "keyValue": {
                            "topLabel": "Timeout Time",
                            "content": f"{str(first_time)} to {str(last_time)}"
                        }
                    }
                ]
            }
        ]
    }
    return card


def format_top_feature_percent(feature_dict, total):
    if not isinstance(feature_dict, dict) or total == 0:
        return ''
    return '\n'.join([f"- {k}: {round((v / total) * 100)}%" for k, v in feature_dict.items()])


def get_unique_tid(start,end):
    query = {
              "runtime_mappings": {
                "trx_status": {
                  "type": "keyword",
                  "script": {
                    "source": """
                      if (doc['document.rc.keyword'].size() != 0) {
                            def list = ["Q1", "Q2", "Q3", "Q4"];
                            def rc = doc['document.rc.keyword'].value;
                            if (list.contains(rc)) {
                                emit("bri_issue");
                            }  
                            else if (doc['document.rd.keyword'].size() != 0){
                                String rd = doc['document.rd.keyword'].value;
                                if (rd == 'TC - Reversal Timeout' || 
                                    rd == 'TC - Timeout (transactionReversalReq : success)' || 
                                    rd == 'TC - Timeout (transactionReversalReq : failed)') {
                                    emit("reversal_timeout");
                                } else if (rd.contains("Timeout") || rd.contains("Time Out")) {
                                    emit("timeout");
                                } else if ((rd == "APPROVED" || rd == "APPROVED - failed to update") && doc['document.rc.keyword'].size() != 0 && doc['document.rc.keyword'].value == "00") {
                                    emit("success");
                                } else if (rd.contains("TC - Q1") || rd.contains("Q1") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q2") || rd.contains("Q2") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q3") || rd.contains("Q3") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q4") || rd.contains("Q4") ) {
                                    emit("bri_issue");
                                } 
                            }
                        }
                        else {
                            if (doc['document.rd.keyword'].size() != 0){
                                String rd = doc['document.rd.keyword'].value;
                                if (rd == 'TC - Reversal Timeout' || 
                                    rd == 'TC - Timeout (transactionReversalReq : success)' || 
                                    rd == 'TC - Timeout (transactionReversalReq : failed)') {
                                    emit("reversal_timeout");
                                } else if (rd.contains("Timeout") || rd.contains("Time Out")) {
                                    emit("timeout");
                                } else if ((rd == "APPROVED" || rd == "APPROVED - failed to update") && doc['document.rc.keyword'].size() != 0 && doc['document.rc.keyword'].value == "00") {
                                    emit("success");
                                } else if (rd.contains("TC - Q1") || rd.contains("Q1") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q2") || rd.contains("Q2") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q3") || rd.contains("Q3") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q4") || rd.contains("Q4") ) {
                                    emit("bri_issue");
                                } 
                            }
                        }
                    """
                  }
                }
              },
              "query": {
                "bool": {
                  "filter": [
                    {
                      "range": {
                        "document.created_at": {
                          "gte": start,
                          "lte": end,
                          "time_zone": "+07:00"
                        }
                      }
                    }
                  ]
                }
              },
              "sort": [
                {
                  "@timestamp": {
                    "order": "asc"
                  }
                }
              ],
              "aggs": {
                "trx_status": {
                  "terms": {
                    "field": "trx_status"
                  }
                },
                "timeout_trx" : {
                  "filter" :{
                    "term" : {
                      "trx_status": "timeout"
                    }
                  },
                  "aggs": {
                    "total_tid" : {
                      "cardinality" : {
                        "field": "document.acq_tid.keyword"
                      }
                    },
                    "top_feature":{
                      "terms": {
                        "field": "document.payment_features.keyword",
                        "size" : 5
                      }
                    }
                  }
                },
                "success_trx" : {
                  "filter" :{
                    "term" : {
                      "trx_status": "success"
                    }
                  },
                  "aggs": {
                    "total_tid" : {
                      "cardinality" : {
                        "field": "document.acq_tid.keyword"
                      }
                    }
                  }
                },
                "bri_host_issue_trx" : {
                  "filter" :{
                    "term" : {
                      "trx_status": "bri_issue"
                    }
                  },
                  "aggs": {
                    "total_tid" : {
                      "cardinality" : {
                        "field": "document.acq_tid.keyword"
                      }
                    },
                    "top_feature":{
                      "terms": {
                        "field": "document.payment_features.keyword",
                        "size" : 5
                      }
                    }
                  }
                }    
              },
              "_source": 'false',
              "track_total_hits": 'true',
              "size": 1
            }
        
    
    es = connect_elasticsearch()
    response = es.search(index="logstash-transaction", body=query) 

    if 'aggregations' in response:
        rows = []

        # Extract doc_counts for all trx_status buckets
        doc_counts = {
            item['key']: item['doc_count']
            for item in response['aggregations'].get('trx_status', {}).get('buckets', [])
        }

        timeout_total_tid = response['aggregations'].get('timeout_trx', {}).get('total_tid', {}).get('value', 0)
        success_total_tid = response['aggregations'].get('success_trx', {}).get('total_tid', {}).get('value', 0)
        bri_issue_tid = response['aggregations'].get('bri_host_issue_trx', {}).get('total_tid', {}).get('value', 0)

        timeout_trx = doc_counts.get('timeout', 0)
        bri_issue_trx = doc_counts.get('bri_issue', 0)
        success_trx = doc_counts.get('success', 0)

        # timeout top features
        top_feature_timeout_buckets = response['aggregations'].get('timeout_trx', {}).get('top_feature', {}).get('buckets', [])
        top_feature_timeout_map = {item['key']: item['doc_count'] for item in top_feature_timeout_buckets}
        formatted_top_feature_timeout = format_top_feature_percent(top_feature_timeout_map, timeout_trx)

        # bri_issue top features
        top_feature_bri_buckets = response['aggregations'].get('bri_host_issue_trx', {}).get('top_feature', {}).get('buckets', [])
        top_feature_bri_map = {item['key']: item['doc_count'] for item in top_feature_bri_buckets}
        formatted_top_feature_bri = format_top_feature_percent(top_feature_bri_map, bri_issue_trx)

        row = {
            'success_trx': success_trx,
            'timeout_trx': timeout_trx,
            'bri_issue_trx': bri_issue_trx,
            'success_unique_tid': success_total_tid,
            'timeout_unique_tid': timeout_total_tid,
            'bri_issue_tid': bri_issue_tid,
            'timeout_top_feature': formatted_top_feature_timeout,
            'bri_top_feature': formatted_top_feature_bri
        }

        rows.append(row)

        df = pd.DataFrame(rows)
        return df


    
def get_data_trx_timeout():
    '''
    Fetch timeout data and return aggregation results as a DataFrame.
    '''
    import pandas as pd
    query = {
          "runtime_mappings": {
            "trx_status": {
              "type": "keyword",
              "script": {
                "source": """
                  if (doc['document.rc.keyword'].size() != 0) {
                            def list = ["Q1", "Q2", "Q3", "Q4"];
                            def rc = doc['document.rc.keyword'].value;
                            if (list.contains(rc)) {
                                emit("bri_issue");
                            }  
                            else if (doc['document.rd.keyword'].size() != 0){
                                String rd = doc['document.rd.keyword'].value;
                                if (rd == 'TC - Reversal Timeout' || 
                                    rd == 'TC - Timeout (transactionReversalReq : success)' || 
                                    rd == 'TC - Timeout (transactionReversalReq : failed)') {
                                    emit("reversal_timeout");
                                } else if (rd.contains("Timeout") || rd.contains("Time Out")) {
                                    emit("timeout");
                                } else if ((rd == "APPROVED" || rd == "APPROVED - failed to update") && doc['document.rc.keyword'].size() != 0 && doc['document.rc.keyword'].value == "00") {
                                    emit("success");
                                } else if (rd.contains("TC - Q1") || rd.contains("Q1") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q2") || rd.contains("Q2") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q3") || rd.contains("Q3") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q4") || rd.contains("Q4") ) {
                                    emit("bri_issue");
                                } 
                            }
                        }
                        else {
                            if (doc['document.rd.keyword'].size() != 0){
                                String rd = doc['document.rd.keyword'].value;
                                if (rd == 'TC - Reversal Timeout' || 
                                    rd == 'TC - Timeout (transactionReversalReq : success)' || 
                                    rd == 'TC - Timeout (transactionReversalReq : failed)') {
                                    emit("reversal_timeout");
                                } else if (rd.contains("Timeout") || rd.contains("Time Out")) {
                                    emit("timeout");
                                } else if ((rd == "APPROVED" || rd == "APPROVED - failed to update") && doc['document.rc.keyword'].size() != 0 && doc['document.rc.keyword'].value == "00") {
                                    emit("success");
                                } else if (rd.contains("TC - Q1") || rd.contains("Q1") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q2") || rd.contains("Q2") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q3") || rd.contains("Q3") ) {
                                    emit("bri_issue");
                                } else if (rd.contains("TC - Q4") || rd.contains("Q4") ) {
                                    emit("bri_issue");
                                } 
                            }
                        }
                """
              }
            }
          },
          "query": {
            "bool": {
              "filter": [
                {
                  "range": {
                    "document.created_at": {
                     "gte": "now-5m/m",
                      "lte": "now",
                      "time_zone": "+07:00"
                    }
                  }
                }
              ]
            }
          },
          "sort": [
            {
              "@timestamp": {
                "order": "asc"
              }
            }
          ],
          "aggs": {
            "created_at": {
              "date_histogram": {
                "field": "document.created_at",
                "fixed_interval": "1m",
                "time_zone": "+07:00"
              },
              "aggs": {
                "trx_status": {
                  "terms": {
                    "field": "trx_status"
                  }
                },
                "timeout_trx" : {
                  "filter" :{
                    "term" : {
                      "trx_status": "timeout"
                    }
                  },
                  "aggs": {
                    "total_tid" : {
                      "cardinality" : {
                        "field": "document.acq_tid.keyword"
                      }
                    },
                    "top_feature":{
                      "terms": {
                        "field": "document.payment_features.keyword",
                        "size" : 5
                      }
                    }
                  }
                },
                "success_trx" : {
                  "filter" :{
                    "term" : {
                      "trx_status": "success"
                    }
                  },
                  "aggs": {
                    "total_tid" : {
                      "cardinality" : {
                        "field": "document.acq_tid.keyword"
                      }
                    }
                  }
                },
                "bri_host_issue_trx" : {
                  "filter" :{
                    "term" : {
                      "trx_status": "bri_issue"
                    }
                  },
                  "aggs": {
                    "total_tid" : {
                      "cardinality" : {
                        "field": "document.acq_tid.keyword"
                      }
                    },
                    "top_feature":{
                      "terms": {
                        "field": "document.payment_features.keyword",
                        "size" : 5
                      }
                    }
                  }
                }
              }
            }
          },
          "_source": 'false',
          "track_total_hits": 'true',
          "size": 1
        }
    
    
    es = connect_elasticsearch()
    response = es.search(index="logstash-transaction", body=query) 
    if 'aggregations' in response:
        agg_buckets = response['aggregations']['created_at']['buckets']
        rows = []

        for bucket in agg_buckets:
            created_at = bucket['key_as_string']
            doc_counts = {
                item['key']: item['doc_count']
                for item in bucket.get('trx_status', {}).get('buckets', [])
            }
            timeout_total_tid = bucket.get('timeout_trx', {}).get('total_tid', {}).get('value', 0)
            success_total_tid = bucket.get('success_trx', {}).get('total_tid', {}).get('value', 0)
            bri_issue_tid = bucket.get('bri_host_issue_trx', {}).get('total_tid', {}).get('value', 0)

            timeout_trx = doc_counts.get('timeout', 0)
            bri_issue_trx = doc_counts.get('bri_issue', 0)

            # timeout top features
            top_feature_timeout_buckets = bucket.get('timeout_trx', {}).get('top_feature', {}).get('buckets', [])
            top_feature_timeout_map = {item['key']: item['doc_count'] for item in top_feature_timeout_buckets}
            formatted_top_feature_timeout = format_top_feature_percent(top_feature_timeout_map, timeout_trx)

            # bri_issue top features
            top_feature_bri_buckets = bucket.get('bri_host_issue_trx', {}).get('top_feature', {}).get('buckets', [])
            top_feature_bri_map = {item['key']: item['doc_count'] for item in top_feature_bri_buckets}
            formatted_top_feature_bri = format_top_feature_percent(top_feature_bri_map, bri_issue_trx)

            row = {
                'created_at': created_at,
                'success_trx': doc_counts.get('success', 0),
                'timeout_trx': timeout_trx,
                'bri_issue_trx': bri_issue_trx,
                'success_unique_tid': success_total_tid,
                'timeout_unique_tid': timeout_total_tid,
                'bri_issue_tid': bri_issue_tid,
                'timeout_top_feature': formatted_top_feature_timeout,
                'bri_top_feature': formatted_top_feature_bri
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        return df
    
def upsert_transaction_data(new_df, csv_path="/project/data/timeout/temp_trx_summary.csv"):
    # Load or initialize the CSV
    if os.path.exists(csv_path):
        existing_df = pd.read_csv(csv_path, parse_dates=["created_at"])
    else:
        existing_df = pd.DataFrame(columns=["created_at", "success_trx", "timeout_trx"])

    new_df["created_at"] = pd.to_datetime(new_df["created_at"])
    existing_df["created_at"] = pd.to_datetime(existing_df["created_at"])

    existing_df.set_index("created_at", inplace=True)
    new_df.set_index("created_at", inplace=True)

    existing_df.update(new_df)
    combined_df = pd.concat([existing_df, new_df[~new_df.index.isin(existing_df.index)]])
    combined_df = combined_df.sort_index()
    
    combined_df.reset_index().to_csv(csv_path, index=False)
    print(f"✅ Upsert complete. File updated: {csv_path}")
    

def upsert_temp_data(
    range_time_curr=None,
    timeout_unique_tid=None,
    success_unique_tid=None,
    bri_issue_tid=None,
    timeout_feature=None,
    bri_feature=None,
    csv_path="/project/data/timeout/temp_sum.csv",
    clear=False
):
    # If clear is requested, empty the file with headers
    if clear:
        empty_df = pd.DataFrame(columns=['range_time_curr', 'timeout_unique_tid', 'success_unique_tid', 'bri_issue_tid', 'timeout_feature', 'bri_feature' ])
        empty_df.to_csv(csv_path, index=False)
        return empty_df

    # Create the new row as a DataFrame
    new_df = pd.DataFrame({
        'range_time_curr': [range_time_curr],
        'timeout_unique_tid': [timeout_unique_tid],
        'success_unique_tid': [success_unique_tid],
        'bri_issue_tid': [bri_issue_tid],
        'timeout_top_feature': [timeout_feature],
        'bri_top_feature': [bri_feature] 
    })

    # Check if CSV exists and has data
    if not os.path.exists(csv_path) or pd.read_csv(csv_path).empty:
        new_df.to_csv(csv_path, index=False)
        return new_df
    else:
        new_df.to_csv(csv_path, index=False)
        return new_df
    
def flush_temp_to_history(first_time, last_time, temp_path="/project/data/timeout/temp_trx_summary.csv", history_path="/project/data/timeout/historical_trx_summary.csv"):
    if not os.path.exists(temp_path):
        print("🚫 Temp file doesn't exist.")
        return

    temp_df = pd.read_csv(temp_path, parse_dates=["created_at"])
    temp_df = temp_df[(temp_df['created_at'] >= first_time) & (temp_df['created_at'] <= last_time)]

    if temp_df.empty:
        print("⚠️ Temp file is empty, nothing to move.")
        return


    if os.path.exists(history_path):
        history_df = pd.read_csv(history_path, parse_dates=["created_at"])
    else:
        history_df = pd.DataFrame(columns=["created_at", "success_trx", "timeout_trx"])

    history_df.set_index("created_at", inplace=True)
    temp_df.set_index("created_at", inplace=True)

    history_df.update(temp_df)
    combined_df = pd.concat([history_df, temp_df[~temp_df.index.isin(history_df.index)]])

    combined_df = combined_df.sort_index()
    combined_df.reset_index().to_csv(history_path, index=False)

    temp_df = pd.DataFrame(columns=["created_at", "success_trx", "timeout_trx"])
    temp_df.to_csv(temp_path, index=False)

    print("✅ Temp data moved to historical & temp cleared.")
    
    
def set_default():
    with open("/project/data/timeout/timeout_time.json", "w") as file:
        json.dump({
            "first_timeout_time": None,
            "first_timeout_status": 0,
            "latest_timeout_time":None,
            "range_time" : "-"
            
        }, file)

def set_variable_timeout(fto, ftos, lto, range_time):
    with open("/project/data/timeout/timeout_time.json", "w") as file:
        json.dump({
            "first_timeout_time": fto,
            "first_timeout_status":ftos,
            "latest_timeout_time":lto,
            "range_time" : range_time
            
        }, file)
        
def load_timeout_time():
    if os.path.exists("/project/data/timeout/timeout_time.json"):
        with open("/project/data/timeout/timeout_time.json", "r") as file:
            data = json.load(file)
            first_timeout_time = data.get("first_timeout_time")
            first_timeout_status = data.get("first_timeout_status")
            latest_timeout_time = data.get("latest_timeout_time")
            range_time = data.get("range_time")

            return  first_timeout_time, first_timeout_status, latest_timeout_time,range_time
        
        
def convert_utc_to_jakarta(time_str):
    """
    Convert UTC ISO timestamp string to Asia/Jakarta time in 'YYYY-MM-DD HH:MM:SS' format.
    """
    return (
        pd.to_datetime(time_str)
        .tz_convert("Asia/Jakarta")
        .strftime("%Y-%m-%d %H:%M:%S")
    )


def convert_jakarta_to_utc(time_str):
    local_tz = pytz.timezone('Asia/Jakarta')
    local_dt = local_tz.localize(date.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S"))
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def trx_timeout_process():
    data = get_data_trx_timeout()
    url = 'https://chat.googleapis.com/v1/spaces/AAQAlaHBu1Q/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=Hz4o76kwv4j1y4YWWZcE6JUC7Nah5utumE9Dj0fgxY4'
    
    if not data.empty:
        latest_row = data.iloc[-1]
        # jika ada data transaksi
        if (latest_row['success_trx'] + latest_row['timeout_trx']) >= 0:
            first_time, first_timeout_status, last_time, range_time = load_timeout_time()
            #jika first_timeout_status == 0
            if first_timeout_status == 0:
                #cek apakah masih ada trx yng melebihi threshbold
                if not data[(data['timeout_unique_tid'] > 30) | (data['bri_issue_tid'] > 30)].empty:
                    filtered_df = data[(data['timeout_unique_tid'] > 30) | (data['bri_issue_tid'] > 30)]
                    data['created_at'] = pd.to_datetime(data['created_at'])
                    
                    first_to = convert_utc_to_jakarta(filtered_df['created_at'].min())
                    last_to_max = convert_utc_to_jakarta(filtered_df['created_at'].max())
                    last_to_dt = date.datetime.strptime(last_to_max, "%Y-%m-%d %H:%M:%S")
                    last_to = last_to_dt.replace(second=59).strftime("%Y-%m-%d %H:%M:%S")
                    
                    first_to_dt = pd.to_datetime(first_to).tz_localize('Asia/Jakarta')
                    last_to_dt = pd.to_datetime(last_to).tz_localize('Asia/Jakarta')
                    
                    #create range time
                    range_time_curr = f"{first_to} to {last_to}"
                    data = data[(data['created_at'] >= first_to_dt) & (data['created_at'] <= last_to_dt)]
                    total_timeout = data['timeout_trx'].sum()
                    total_success = data['success_trx'].sum()
                    timeout_unique_tid = data['timeout_unique_tid'].sum()
                    success_unique_tid = data['success_unique_tid'].sum()
                    bri_issue_tid = data['bri_issue_tid'].sum()
                    timeout_feature = data['timeout_top_feature'].iloc[-1]
                    bri_feature = data['bri_top_feature'].iloc[-1]
                    
                    #save timeout to temp timeout table
                    upsert_transaction_data(data)

                    #send notification
                    card = card_event(first_to,last_to,1,timeout_unique_tid,success_unique_tid,bri_issue_tid,timeout_feature,bri_feature)
                    send_webhook_notification(url, card)

                    #set variable timeout
                    set_variable_timeout(first_to,1,last_to,range_time_curr)
                    
                    #input temp data
                    upsert_temp_data(range_time_curr,timeout_unique_tid,success_unique_tid,bri_issue_tid,timeout_feature,bri_feature)
                    
                    
                #jika tidak melebihi threshold
                else:
                    print("no timeout")
            #jika first_timeout_status == 1,
            else:
                if not data[(data['timeout_unique_tid'] > 30) | (data['bri_issue_tid'] > 30)].empty:
                    first_to = first_time
                    filtered_df = data[(data['timeout_unique_tid'] > 30) | (data['bri_issue_tid'] > 30)]
                    last_to_max = convert_utc_to_jakarta(filtered_df['created_at'].max())
                    last_to_dt = date.datetime.strptime(last_to_max, "%Y-%m-%d %H:%M:%S")
                    last_to = last_to_dt.replace(second=59).strftime("%Y-%m-%d %H:%M:%S")
                    
                    #create range time
                    range_time_curr = f"{first_to} to {last_to}"
                    prev_range_time = range_time
                    
                    upsert_transaction_data(data)

                    #get data from summary
                    data_sum = pd.read_csv("/project/data/timeout/temp_trx_summary.csv")
                    filtered_data_sum = data_sum[(data_sum['created_at'] >= first_to) & (data_sum['created_at'] <= last_to)]
                    total_timeout = filtered_data_sum['timeout_trx'].sum()
                    total_success = filtered_data_sum['success_trx'].sum()
                    
                    
                    #get unique tid
                    start = convert_jakarta_to_utc(first_to)
                    end = convert_jakarta_to_utc(last_to)
                    data_tid = get_unique_tid(start,end)
                    timeout_unique_tid = 0
                    if 'timeout_unique_tid' in data_tid.columns:
                        timeout_unique_tid = data_tid['timeout_unique_tid'].sum()
                    
                    success_unique_tid = 0
                    if 'success_unique_tid' in data_tid.columns:
                        success_unique_tid = data_tid['success_unique_tid'].sum()
                    
                    bri_issue_tid = 0
                    if 'bri_issue_tid' in data_tid.columns:
                        bri_issue_tid = data_tid['bri_issue_tid'].sum()
                        
                    timeout_feature = "-"
                    if 'timeout_top_feature' in data_tid.columns:
                        timeout_feature = data_tid['timeout_top_feature'].iloc[-1]
                        
                    bri_feature = "-"
                    if 'bri_top_feature' in data_tid.columns:
                        bri_feature = data_tid['bri_top_feature'].iloc[-1]
                    
                    if (range_time_curr != prev_range_time):
                        duration = date.datetime.strptime(last_to, "%Y-%m-%d %H:%M:%S") - date.datetime.strptime(first_to, "%Y-%m-%d %H:%M:%S")
                        duration =  int(duration.total_seconds() / 60) + 1

                        card = card_event(first_to,last_to,duration,timeout_unique_tid,success_unique_tid,bri_issue_tid,timeout_feature,bri_feature)
                        send_webhook_notification(url, card)
                        set_variable_timeout(first_to,1,last_to,range_time_curr)
                        upsert_temp_data(range_time_curr,timeout_unique_tid,success_unique_tid,bri_issue_tid,timeout_feature,bri_feature)
                    else :
                        print("do nothing")
                else:
                    first_time, first_timeout_status, last_time, range_time = load_timeout_time()
                    data_sum = pd.read_csv("/project/data/timeout/temp_trx_summary.csv")
                    filtered_data_sum = data_sum[(data_sum['created_at'] >= first_time) & (data_sum['created_at'] <= last_time)]
                    
                    #get summary
                    first_to = date.datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S")
                    last_to = date.datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
                    duration = last_to - first_to
                    duration =  int(duration.total_seconds() / 60) + 1
                    
                    total_timeout = filtered_data_sum['timeout_trx'].sum()
                    total_success = filtered_data_sum['success_trx'].sum() 
                    
                    #get unique tid
                    start = convert_jakarta_to_utc(first_time)
                    end = convert_jakarta_to_utc(last_time)
                    data_tid = get_unique_tid(start,end)
                    timeout_unique_tid = 0
                    if 'timeout_unique_tid' in data_tid.columns:
                        timeout_unique_tid = data_tid['timeout_unique_tid'].sum()
                    
                    success_unique_tid = 0
                    if 'success_unique_tid' in data_tid.columns:
                        success_unique_tid = data_tid['success_unique_tid'].sum()
                        
                    bri_issue_tid = 0
                    if 'bri_issue_tid' in data_tid.columns:
                        bri_issue_tid = data_tid['bri_issue_tid'].sum()
                        
                    timeout_feature = "-"
                    if 'timeout_top_feature' in data_tid.columns:
                        timeout_feature = data_tid['timeout_top_feature'].iloc[-1]
                        
                    bri_feature = "-"
                    if 'bri_top_feature' in data_tid.columns:
                        bri_feature = data_tid['bri_top_feature'].iloc[-1]
                    
                     #check the unique tid
                    latest_temp = pd.read_csv("/project/data/timeout/temp_sum.csv")
                    latest_timeout_tid = 0
                    if 'timeout_unique_tid' in latest_temp.columns:
                        latest_timeout_tid = latest_temp.loc[0,'timeout_unique_tid']
                    
                    latest_success_tid = 0
                    if 'success_unique_tid' in latest_temp.columns:
                        latest_success_tid = latest_temp.loc[0,'success_unique_tid']
                    
                    latest_bri_tid = 0
                    if 'bri_issue_tid' in latest_temp.columns:
                        latest_bri_tid = latest_temp.loc[0,'bri_issue_tid']
                        
                    latest_timeout_feature = "-"
                    if 'timeout_top_feature' in latest_temp.columns:
                        latest_timeout_feature = latest_temp.loc[0,'timeout_top_feature']
                    
                    latest_bri_feature = "-"
                    if 'bri_top_feature' in latest_temp.columns:
                        latest_bri_feature = latest_temp.loc[0,'bri_top_feature']
                    
                    card = ""
                    if(timeout_unique_tid < latest_timeout_tid or success_unique_tid < latest_success_tid or bri_issue_tid < latest_bri_tid) :
                        card = card_summary(first_to,last_to,duration,latest_timeout_tid,latest_success_tid,latest_bri_tid,latest_timeout_feature,latest_bri_feature)
                    else:
                        card = card_summary(first_to,last_to,duration,timeout_unique_tid,success_unique_tid,bri_issue_tid,timeout_feature,bri_feature)
                    
                    send_webhook_notification(url, card)
                    flush_temp_to_history(first_time, last_time) 
                    upsert_temp_data(upsert_temp_data(clear=True))
                    time.sleep(10)
                    set_default()
        else:
            print("Latest data has no transactions.")
            
            
    else:
        first_time, first_timeout_status, last_time, range_time = load_timeout_time()
        if first_timeout_status == 1:
            data_sum = pd.read_csv("/project/data/timeout/temp_trx_summary.csv")
            filtered_data_sum = data_sum[(data_sum['created_at'] >= first_time) & (data_sum['created_at'] <= last_time)]
            #get summary
            first_to_dt = date.datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S")
            last_to_dt= date.datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
            duration = last_to_dt - first_to_dt
            duration =  int(duration.total_seconds() / 60) + 1
            
            total_timeout = filtered_data_sum['timeout_trx'].sum()
            total_success = filtered_data_sum['success_trx'].sum()
            
            #get unique tid
            start = convert_jakarta_to_utc(first_time)
            end = convert_jakarta_to_utc(last_time)
            data_tid = get_unique_tid(start,end)
            
            timeout_unique_tid = 0
            if 'timeout_unique_tid' in data_tid.columns:
                timeout_unique_tid = data_tid['timeout_unique_tid'].sum()

            success_unique_tid = 0
            if 'success_unique_tid' in data_tid.columns:
                success_unique_tid = data_tid['success_unique_tid'].sum()
            
            bri_issue_tid = 0
            if 'bri_issue_tid' in data_tid.columns:
                bri_issue_tid = data_tid['bri_issue_tid'].sum()

            timeout_feature = "-"
            if 'timeout_top_feature' in data_tid.columns:
                timeout_feature = data_tid['timeout_top_feature'].iloc[-1]

            bri_feature = "-"
            if 'bri_top_feature' in data_tid.columns:
                bri_feature = data_tid['bri_top_feature'].iloc[-1]
                    
            
            latest_temp = pd.read_csv("/project/data/timeout/temp_sum_.csv") 
            latest_timeout_tid = 0
            if 'timeout_unique_tid' in latest_temp.columns:
                latest_timeout_tid = latest_temp.loc[0,'timeout_unique_tid']

            latest_success_tid = 0
            if 'success_unique_tid' in latest_temp.columns:
                latest_success_tid = latest_temp.loc[0,'success_unique_tid']
                
            latest_bri_tid = 0
            if 'bri_issue_tid' in latest_temp.columns:
                latest_bri_tid = latest_temp.loc[0,'bri_issue_tid']

            latest_timeout_feature = "-"
            if 'timeout_top_feature' in latest_temp.columns:
                latest_timeout_feature = latest_temp.loc[0,'timeout_top_feature']

            latest_bri_feature = "-"
            if 'bri_top_feature' in latest_temp.columns:
                latest_bri_feature = latest_temp.loc[0,'bri_top_feature']
                
            card = ""
            if(timeout_unique_tid < latest_timeout_tid or success_unique_tid < latest_success_tid or bri_issue_tid < latest_bri_tid) :
                card = card_summary(first_to_dt,last_to_dt,duration,latest_timeout_tid,latest_success_tid,latest_bri_tid,latest_timeout_feature,latest_bri_feature)
            else:
                card = card_summary(first_to_dt,last_to_dt,duration,timeout_unique_tid,success_unique_tid,bri_issue_tid,timeout_feature,bri_feature)

            send_webhook_notification(url, card)
            flush_temp_to_history(first_time, last_time) 
            upsert_temp_data(upsert_temp_data(clear=True))
            set_default()
        else:
            print("no data")
        
        
trx_timeout_process()
