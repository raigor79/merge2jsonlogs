import datetime
import os
from typing import Iterable, Union, Generator
from tqdm import tqdm
from loguru import logger
import sys
import argparse
import json
import time

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
LENGTH_CHUNK = 255


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


def make_dir(path: str) -> None:
    """ Directories creation function
    arguments:
    path - full path make directory
    """
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except OSError:
            logger.error(f"Create directories {path} failed")


def convert_str_to_time(string: str, format=TIMESTAMP_FORMAT) -> time.struct_time:
    return time.strptime(string, format)


def load_line(log_file_path: str) -> Generator:
    """Load data from the file log on the specified path
    arguments:
    log_file_path - full path to the file log 
    """
    with open(log_file_path, 'rt', encoding='utf-8') as log_file:
        for line in log_file:
            yield line


def read_value_from_generator(generator: Generator, fieldstr: str):
    try:
        line = next(generator)
        time_line = convert_str_to_time(json.loads(line)[fieldstr])
    except StopIteration:        
        return None, True, None
    return line, False, time_line


def generator_merge_logs(generator_one: Generator, generator_two: Generator) -> Generator:
    """ Merge generator """
    line_a, stop_a, time_line_a = read_value_from_generator(
        generator_one, 
        'timestamp'
        )
    line_b, stop_b, time_line_b = read_value_from_generator(
        generator_two, 
        'timestamp'
        )   
    while not stop_a or not stop_b:

        if not stop_a and not stop_b:
            if time_line_a >= time_line_b:
                line_min_time = line_b
                line_b, stop_b, time_line_b = read_value_from_generator(
                    generator_two,
                    'timestamp'
                    )
                
            elif time_line_a < time_line_b:
                line_min_time = line_a
                line_a, stop_a, time_line_a = read_value_from_generator(
                    generator_one, 
                    'timestamp'
                    )

        elif stop_a:
            line_min_time = line_b
            line_b, stop_b, time_line_b = read_value_from_generator(
            generator_two,
            'timestamp'
            )

        elif stop_b:
            line_min_time = line_a     
            line_a, stop_a, time_line_a = read_value_from_generator(
            generator_one, 
            'timestamp'
            )

        yield line_min_time


@logger.catch()
def merge_log(args_cmd) -> None:
    """Function merge two files log in one outfile
    """ 
    gen_log_a = load_line(args_cmd.path_to_log1)
    gen_log_b = load_line(args_cmd.path_to_log2)
    path_, name_log_file = os.path.split(args_cmd.out_log)
    temp_path_out = os.path.join(path_, "." + name_log_file)
    with open(temp_path_out, 'w') as merge_file_log:
        chunck = []
        for string_line in generator_merge_logs(gen_log_a, gen_log_b):
            if len(chunck) < LENGTH_CHUNK:
                chunck.append(string_line)
            else:
                merge_file_log.writelines(chunck)
                chunck = []
        else:
            if chunck:
                merge_file_log.writelines(chunck)
    os.rename(temp_path_out, args_cmd.out_log)


def main() -> None:
    args_cmd = parsing_args()

    logger.add(
        sys.stderr, 
        format="{time} {level} {message}", 
        filter="my_module", 
        level="INFO"
    )
    
    logger.info(f"Start with args:{args_cmd}")
    path_out_log, _ = os.path.split(args_cmd.out_log)
    make_dir(path_out_log)
    merge_log(args_cmd)
    logger.info("Merge logs - Ok")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logger.error(f'Error {error}')
        raise error
    except KeyboardInterrupt:
        pass
