from tqdm import tqdm
from loguru import logger
import sys
import argparse
import json
import time
from optparse import OptionParser

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


def main():
    logger.add(sys.stderr, format="{time} {level} {message}", filter="my_module", level="INFO")
    args_cmd = parsing_args()
    logger.info(f"Start with args:{args_cmd}")
    


if __name__ == "__main__":
    main()