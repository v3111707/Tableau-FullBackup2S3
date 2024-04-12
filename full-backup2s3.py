#!/usr/bin/env python3

import subprocess
import time
import sys
import logging
import os
import configparser
import boto3
import socket
from logging.handlers import TimedRotatingFileHandler

LOGGER_NAME = 'main'
CONFIG_FILE = 'config.ini'
POSSIBLE_COMMANDS = ['backup', 'upload']
HELP_MESSAGE = [
    'The full-backup2s3 is used to start "tsm maintenance backup", upload backups to the S3 and send result to Zabbix.',
    'It\'s uses only one non standard libary: boto3',
    'Usage: full-backup2s3  ',
    'Commands:',
    '   backup    --  Runs "tsm maintenance backup" and after completion starts the "upload" command.',
    '   upload    --  Upload all *.tsbak files in the backup folder to S3 and remove its',
]


def init_logger(name: str = None, debug: bool = False) -> logging.Logger:
    logger = logging.getLogger(name)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s: %(message)s'))
    logger.addHandler(sh)
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    return logger


def init_filelogger(filename: str,
                    when: str,
                    interval: int,
                    backup_count: int,
                    name: str = None,
                    debug: bool = False) -> None:
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    script_dir = os.path.dirname(os.path.realpath(__file__))
    filename_path = os.path.join(script_dir, filename)
    backup_count = int(backup_count)
    interval = int(interval)
    handler = TimedRotatingFileHandler(filename=filename_path,
                                       when=when,
                                       interval=interval,
                                       backupCount=backup_count)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)


def get_timestamp() -> str:
    return time.strftime('%Y%m%d-%H%M%S')


def get_config(path: str) -> configparser.ConfigParser:
    logger = logging.getLogger(LOGGER_NAME)
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, path)
    config = configparser.ConfigParser()
    logger.debug(f'Reading config file: {path}')
    if not os.path.isfile(config_path):
        msg = f'Config file not found: {path}. Exit'
        logger.debug(msg)
        sys.exit(msg)
    try:
        config.read(config_path)
    except Exception as e:
        logger.exception(e)
        msg = f'Exception while parsing the config file: {path}'
        logger.error(msg)
        sys.exit(msg)
    return config


def print_help() -> None:
    [print(i) for i in HELP_MESSAGE]


def send_to_zabbix(key: str, value: int, config_file: str) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    args = [
        'zabbix_sender',
        '-c',
        config_file,
        '-s',
        socket.gethostname(),
        '-k',
        key,
        '-o',
        str(value)
    ]
    logger.info(f'Send to Zabbix" {key=},  {value=}')
    logger.debug(f'Running command: {" ".join(args)}')
    completed_process = subprocess.run(args)


def start_backup(backup_file: str,
                 append_timestamp: bool = False,
                 multithreaded: bool = False
                 ):
    logger = logging.getLogger(LOGGER_NAME)
    if append_timestamp:
        backup_file += '_'+  get_timestamp()
    args = f'source /etc/profile.d/tableau_server.sh; tsm maintenance backup --ignore-prompt --file {backup_file}'

    if multithreaded:
        args += ' --multithreaded'
    logger.debug(f'subprocess.run: "{args}"')
    start_backup_time = time.time()
    completed_process = subprocess.run(args, capture_output=True, shell=True)
    tsm_backup_duration_time = int(time.time() - start_backup_time)
    return (completed_process.returncode,
            completed_process.stdout.decode(),
            completed_process.stderr.decode(),
            tsm_backup_duration_time)


def main():
    if len(sys.argv) == 1 or sys.argv[1] not in POSSIBLE_COMMANDS:
        print_help()
        sys.exit('\nMissing or unsupported command')

    logger = init_logger(name=LOGGER_NAME,
                         debug='-d' in sys.argv)


    config = get_config(CONFIG_FILE)
    backup_conf = config['Backup']
    aws_conf = config['AWS']
    zab_conf = config['Zabbix'] if 'Zabbix' in config.sections() else None

    if 'Logging' in config.sections():
        log_conf = config['Logging']
        init_filelogger(name=LOGGER_NAME,
                        filename=log_conf['filename'],
                        when=log_conf['when'],
                        interval=log_conf.getint('interval'),
                        backup_count=log_conf.getint('backup_count'),
                        debug=log_conf.getboolean('debug'))

    #BACKUP
    if sys.argv[1] == 'backup':
        logger.info('Starting backup')

        tsm_exit_code, tsm_stdout,  tsm_stderr, tsm_backup_duration = start_backup(
            backup_file = backup_conf['backup_file'],
            append_timestamp = backup_conf.getboolean('append_timestamp'),
            multithreaded = backup_conf.getboolean('multithreaded')
        )
        tsm_backup_result_code = 0  if 'Backup written to ' in tsm_stdout else 1

        logger.debug(f'{tsm_stdout=},\n {tsm_stderr=},\n {tsm_exit_code=}')
        logger.info(f'{tsm_backup_duration=} sec,\n {tsm_backup_result_code=}')

        if zab_conf:
            if tsm_backup_result_code == 0:
                send_to_zabbix(key='tsm_backup_duration',
                               value=int(tsm_backup_duration),
                               config_file=zab_conf['config_file'])
            send_to_zabbix(key='tsm_backup_result_code',
                           value=tsm_backup_result_code,
                           config_file=zab_conf['config_file'])
            send_to_zabbix(key='tsm_exit_code',
                           value=tsm_exit_code,
                           config_file=zab_conf['config_file'])

        if tsm_backup_result_code != 0:
            logger.error('tsm exit code isn\'t zero:\n' + tsm_stdout +   tsm_stderr)

    # UPLOAD
    logger.info('Starting upload')
    if sys.argv[1] in ['backup', 'upload']:
        s3_client = boto3.client(
            service_name='s3',
            region_name=aws_conf['region_name'],
            aws_access_key_id=aws_conf['aws_access_key_id'],
            aws_secret_access_key=aws_conf['aws_secret_access_key']
        )

        backup_dir = backup_conf['backup_dir']
        for dir_entry in os.scandir(backup_dir):
            if dir_entry.is_file() and dir_entry.name.endswith('.tsbak'):
                backup_file_size = int(os.stat(dir_entry.path).st_size )
                process = subprocess.Popen([
                    'md5sum',
                    dir_entry.name,
                ],
                    cwd=backup_dir,
                    stdout=subprocess.PIPE
                )

                upload_result_code = 0
                upload_duration = 0
                logger.info(f'Uploading file "{dir_entry.name}", '
                             f'size: {int(backup_file_size / (1024 * 1024))} MB '
                             f'to {aws_conf["bucket_name"]}')
                start_upload_time = time.time()
                try:
                    s3_client.upload_file(dir_entry.path, aws_conf['bucket_name'], dir_entry.name)
                except Exception as e:
                    # logger.exception(e)
                    logger.error(e)
                    upload_result_code = 1
                else:
                    upload_duration = int(time.time() - start_upload_time)
                    logger.info(f'Remove: {dir_entry.path}')
                    os.remove(dir_entry.path)

                md5sum_output = [l.decode() for l in process.communicate() if l]
                md5sum = ''.join(md5sum_output)
                logger.info(f'Uploading md5sum: "{dir_entry.name}.md5sum.txt" to {aws_conf["bucket_name"]}", ')
                s3_client.put_object(
                    Body=md5sum,
                    Bucket=aws_conf['bucket_name'],
                    Key=f'{dir_entry.name}.md5sum.txt'
                )

                if zab_conf and not upload_result_code:
                    send_to_zabbix(key='backup_file_size',
                                   value=backup_file_size,
                                   config_file=zab_conf['config_file'])
                    send_to_zabbix(key='upload_result_code',
                                   value=upload_result_code,
                                   config_file=zab_conf['config_file'])
                    send_to_zabbix(key='upload_duration',
                                   value=upload_duration,
                                   config_file=zab_conf['config_file'])
    logger.info('End')


if __name__ == '__main__':
    main()
