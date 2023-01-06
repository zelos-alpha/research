from datetime import datetime, timedelta
import os
import time
from enum import Enum

import common.constants as constants


class ChainType(Enum):
    Ethereum = 1
    Polygon = 2


def get_table_name(chain_type: ChainType) -> str:
    match chain_type:
        case ChainType.Polygon:
            return "public-data-finance.crypto_polygon.logs"
        case ChainType.Ethereum:
            return "bigquery-public-data.crypto_ethereum.logs"
        case _:
            raise RuntimeError("chain type {} is not supported by BigQuery", chain_type)


def download_bigquery_pool_event_matic(contract_address: str, date_begin: datetime, date_end: datetime,
                                       data_save_path: os.path, chain_type: ChainType = ChainType.Polygon):
    if not os.path.exists(data_save_path):
        os.makedirs(data_save_path)
    # iter date=> call download one day => save to date by date.
    contract_address = contract_address.lower()
    date_generated = [date_begin + timedelta(days=x) for x in range(0, (date_end - date_begin).days)]
    for one_day in date_generated:
        time_start = time.time()
        df = download_bigquery_pool_event_matic_oneday(contract_address, one_day, chain_type)
        date_str = one_day.strftime("%Y-%m-%d")
        file_name = f"{contract_address}-{date_str}.csv"
        df.to_csv(data_save_path + "//" + file_name, header=True, index=False)
        time_end = time.time()
        print('Day: {}, time cost {} s, data count: {}'.format(date_str, time_end - time_start, len(df.index)))


def download_bigquery_pool_event_matic_oneday(contract_address, one_date, chain_type: ChainType = ChainType.Polygon):
    from google.cloud import bigquery
    client = bigquery.Client()
    query = f"""
SELECT block_number, transaction_hash, block_timestamp, transaction_index  as pool_tx_index, log_index pool_log_index, topics as pool_topics, DATA as pool_data, [] as proxy_topics, '' as proxy_data,null as proxy_log_index
        FROM {get_table_name(chain_type)}
        WHERE  topics[SAFE_OFFSET(0)] in ('{constants.SWAP_KECCAK}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{contract_address}"

union all

select pool.block_number, pool.transaction_hash, pool.block_timestamp, pool.transaction_index as pool_tx_index, pool.log_index as pool_log_index,pool.topics as pool_topics, pool.DATA as pool_data,  proxy.topics as proxy_topics, proxy.DATA as proxy_data, proxy.log_index as proxy_log_index from
(SELECT block_number, transaction_hash, block_timestamp, transaction_index, log_index, topics, DATA FROM {get_table_name(chain_type)}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.MINT_KECCAK}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{contract_address}") as pool
left join 
(SELECT transaction_hash, topics, DATA,log_index FROM {get_table_name(chain_type)}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.INCREASE_LIQUIDITY}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{constants.PROXY_CONTRACT_ADDRESS}") as proxy
on pool.transaction_hash=proxy.transaction_hash 

union all

select pool.block_number, pool.transaction_hash, pool.block_timestamp, pool.transaction_index as pool_tx_index, pool.log_index as pool_log_index,pool.topics as pool_topics, pool.DATA as pool_data,  proxy.topics as proxy_topics, proxy.DATA as proxy_data, proxy.log_index as proxy_log_index from
(SELECT block_number, transaction_hash, block_timestamp, transaction_index, log_index, topics, DATA FROM {get_table_name(chain_type)}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.COLLECT_KECCAK}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{contract_address}") as pool
left join 
(SELECT transaction_hash, topics, DATA,log_index FROM {get_table_name(chain_type)}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.COLLECT}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{constants.PROXY_CONTRACT_ADDRESS}") as proxy
on pool.transaction_hash=proxy.transaction_hash 

union all

select pool.block_number, pool.transaction_hash, pool.block_timestamp, pool.transaction_index as pool_tx_index, pool.log_index as pool_log_index,pool.topics as pool_topics, pool.DATA as pool_data,  proxy.topics as proxy_topics, proxy.DATA as proxy_data, proxy.log_index as proxy_log_index from
(SELECT block_number, transaction_hash, block_timestamp, transaction_index, log_index, topics, DATA FROM {get_table_name(chain_type)}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.BURN_KECCAK}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{contract_address}") as pool
left join 
(SELECT transaction_hash, topics, DATA,log_index FROM {get_table_name(chain_type)}
        WHERE topics[SAFE_OFFSET(0)] in ('{constants.DECREASE_LIQUIDITY}')
            AND DATE(block_timestamp) >=  DATE("{one_date}") AND DATE(block_timestamp) <=  DATE("{one_date}") AND address = "{constants.PROXY_CONTRACT_ADDRESS}") as proxy
on pool.transaction_hash=proxy.transaction_hash     
"""
    query_job = client.query(query)  # Make an API request.
    result = query_job.to_dataframe(create_bqstorage_client=False)
    return result
