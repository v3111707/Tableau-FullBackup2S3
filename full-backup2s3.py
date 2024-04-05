#!/usr/bin/env python3

import subprocess
import time
import sys
import logging
import os
import configparser

LOGGER_NAME = 'main'
CONFIG_FILE = 'config.ini'
POSSIBLE_COMMANDS = ['backup', 'upload', 'zab-test']
HELP_MESSAGE = [
    'Usage: full-backup2s3  ',
    'Commands:',
    '   backup  --  Start backup process and upload it to S3'
]


def init_logger(name: str = None, debug: bool = False, ) -> logging.Logger:
    logger = logging.getLogger(name)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s: %(message)s'))
    logger.addHandler(sh)
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    return logger


def get_config(path: str) -> configparser.ConfigParser:
    logger = logging.getLogger(LOGGER_NAME)
    config = configparser.ConfigParser()
    logger.debug(f'Reading config file: {path}')
    if not os.path.isfile(path):
        sys.exit(f'Config file not found: {path}')
    try:
        config.read(path)
    except Exception as e:
        logger.exception(e)
        sys.exit(f'Exception while parsing the config file: {path}')
    return config


def print_help() -> None:
    [print(i) for i in HELP_MESSAGE]


def run_subprocess(args: list) -> tuple:
    logger = logging.getLogger(LOGGER_NAME)
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = ''

    for line in iter(process.stdout.readline, ""):

        logger.info(line.decode().strip())
        output += line.decode()

    process.wait()
    exit_code = process.returncode

    return exit_code, output, process.stderr


def start_backup(backup_file: str, append_date: bool = False):
    logger = logging.getLogger(LOGGER_NAME)
    command = 'tsm'
    args = [
        command,
        'maintenance',
        'backup',
        '--file',
        backup_file
    ]
    if append_date:
        args.append('--append-date')

    # args = [
    #     'ping',
    #     '8.8.8.8'
    # ]
    logger.info(f'Run "{args[0]}" with arguments: "{" ".join(args[1:])}"')
    # result = subprocess.run(args, capture_output=True)
    returncode, stdout,  stdout= run_subprocess(args)
    return returncode, stdout, stdout


def main():
    logger = init_logger(name=LOGGER_NAME,
                         debug='-d' in sys.argv)

    config = get_config(CONFIG_FILE)

    if len(sys.argv) == 1 or sys.argv[1] not in POSSIBLE_COMMANDS:
        print_help()
        sys.exit('\nMissing or unsupported command')

    if sys.argv[1] == 'backup':
        start_backup_time = time.time()
        backup_conf = config['Backup']
        logger.info('Starting backup')
        return_code, stdout,  stderr = start_backup(**dict(backup_conf.items()))
        logger.debug(f'{return_code=} {stdout=} {stderr=}')
        logger.info("backup time: %s seconds" % (time.time() - start_backup_time))

    start_time = time.time()

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main()
