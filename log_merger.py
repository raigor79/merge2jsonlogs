import os
from tqdm import tqdm
from loguru import logger
import sys
import argparse
import json
import time
from optparse import OptionParser

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def parsing_args()->argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tool to merge two logs of json")
    
    parser.add_argument(
        'path_to_log1',
        metavar='<PATH TO LOG1>',
        type=str,
        help='path to dir with file log 1'        
     )

    parser.add_argument(
        'path_to_log2',
        metavar='<PATH TO LOG2>',
        type=str,
        help='path to dir with file log 2'         
    )   

    parser.add_argument(
        '-o', '--out_log',
        type=str,
        help='path to dir with have merged file log', 
        default='./DIR/log.jsonl',       
    )   
    return parser.parse_args()


def make_dir(path):
    """ Directories creation function
    arguments:
    path - full path make directory
    """
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except OSError:
            logger.error(f"Create directories {path} failed")


def convert_from_json(string):
    return json.loads(string)


def convert_str_to_time(string, format=TIMESTAMP_FORMAT):
    return time.strptime(string, format)


def load_line(log_file_path):
    """Load data from the file log on the specified path
    arguments:
    log_file_path - full path to the file log 
    """
    with open(log_file_path, 'rt', encoding='utf-8') as log_file:
        for line in tqdm(log_file):
            timestamp = convert_str_to_time(convert_from_json(line)['timestamp'])
            yield line


def main():
    logger.add(
        sys.stderr, 
        format="{time} {level} {message}", 
        filter="my_module", 
        level="INFO"
    )
    args_cmd = parsing_args()
    logger.info(f"Start with args:{args_cmd}")
    path_out_log, name_log_file = os.path.split(args_cmd.out_log)
    make_dir(path_out_log)
    gen_log_a = load_line(args_cmd.path_to_log1)
    gen_log_b = load_line(args_cmd.path_to_log2)
    for i in gen_log_a:
        pass


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logger.error(f'Error {error}')