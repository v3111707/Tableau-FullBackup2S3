#!/usr/bin/env python3

import subprocess
import time
import sys
import logging
import os
import configparser
import boto3
import socket

LOGGER_NAME = 'main'
CONFIG_FILE = 'config.ini'
POSSIBLE_COMMANDS = ['backup', 'upload', 'zab-test']
HELP_MESSAGE = [
    'The full-backup2s3 is used to start "tsm maintenance backup", upload backups to the S3 and send result to Zabbix.',
    'It\'s uses only one non standard libary: boto3',
    'Usage: full-backup2s3  ',
    'Commands:',
    '   backup    --  Runs "tsm maintenance backup" and after completion starts the "upload" command.',
    '   upload    --  Upload all *.tsbak files in the backup folder to S3 and remove its',
    '   zab-test  --  Sends in all items the "1" '
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


def get_timestamp() -> str:
    return time.strftime('%Y%m%d-%H%M%S')


def get_config(path: str) -> configparser.ConfigParser:
    script_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(script_dir, path)
    logger = logging.getLogger(LOGGER_NAME)
    config = configparser.ConfigParser()
    logger.debug(f'Reading config file: {path}')
    if not os.path.isfile(config_path):
        sys.exit(f'Config file not found: {path}')
    try:
        config.read(config_path)
    except Exception as e:
        logger.exception(e)
        sys.exit(f'Exception while parsing the config file: {path}')
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
    logger.debug(f'Running command: {" ".join(args)}')
    completed_process = subprocess.run(args)


def start_backup(backup_file: str,
                 append_timestamp: bool = False,
                 multithreaded: bool = False
                 ):
    logger = logging.getLogger(LOGGER_NAME)
    if append_timestamp:
        backup_file += '-'+  get_timestamp()
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
    logger = init_logger(name=LOGGER_NAME,
                         debug='-d' in sys.argv)

    config = get_config(CONFIG_FILE)
    backup_conf = config['Backup']
    aws_conf = config['AWS']
    zab_conf = config['Zabbix'] if 'Zabbix' in config.sections() else None

    if len(sys.argv) == 1 or sys.argv[1] not in POSSIBLE_COMMANDS:
        print_help()
        sys.exit('\nMissing or unsupported command')

    #BACKUP
    if sys.argv[1] == 'backup':
        logger.info('Starting backup')

        tsm_return_code, tsm_stdout,  tsm_stderr, tsm_backup_duration = start_backup(
            backup_file = backup_conf['backup_file'],
            append_timestamp = backup_conf.getboolean('append_timestamp'),
            multithreaded = backup_conf.getboolean('multithreaded')
        )

        # # ToDo
        # succsess_tmp = (0,
        #                 "The previous GenerateBackupJob did not succeed after running for 6 minute(s).\nThe last successful run of GenerateBackupJob took 3 minute(s).\n\nJob id is '10', timeout is 1440 minutes.\n7% - Starting the Active Repository instance, File Store, and Cluster Controller.\nRunning - Waiting for the Active Repository, File Store, and Cluster Controller to start.\r                                                                                         \r14% - Waiting for the Active Repository, File Store, and Cluster Controller to start.\nRunning - Installing backup services.\r                                     \r21% - Installing backup services.\nRunning - Estimating required disk space.\r                                         \r28% - Estimating required disk space.\n35% - Gathering disk space information from all nodes.\n42% - Analyzing disk space information.\n50% - Checking if sufficient disk space is available on all nodes.\n57% - Backing up configuration.\nRunning - [Backing up object storage data.] [Backing up database.]\r                                                                  \r64% - Backing up object storage data.\nRunning - Backing up database.\r                              \r71% - Backing up database.\nRunning - Assembling the tsbak archive. Processing file 1 of 35.\r                                                                \r78% - Assembling the tsbak archive.\n85% - Stopping the Active Repository if necessary.\nRunning - Waiting for the Active Repository to stop if necessary.\r                                                                 \r92% - Waiting for the Active Repository to stop if necessary.\nRunning - Uninstalling backup services.\r                                       \r100% - Uninstalling backup services.\r                                    \r100% - Uninstalling backup services.\nBackup written to '/var/opt/tableau/tableau_server/data/tabsvc/files/backups/ts_backup-2024-04-08.tsbak' on the controller node.\n",
        #                 '',
        #                 69)
        # tsm_return_code, tsm_stdout, tsm_stderr, tsm_backup_duration = succsess_tmp

        logger.debug(f'{tsm_stdout=},\n {tsm_stderr=},\n {tsm_backup_duration=},\n {tsm_return_code=}')

        if zab_conf:
            send_to_zabbix(key='tsm_backup_duration',
                           value=int(tsm_backup_duration),
                           config_file=zab_conf['config_file'])
            send_to_zabbix(key='tsm_backup_exitcode',
                           value=tsm_return_code,
                           config_file=zab_conf['config_file'])

        if tsm_return_code != 0:
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

                logger.info(f'Processing "{dir_entry.name}", size: {backup_file_size / (1024 * 1024)} MB')
                process = subprocess.Popen([
                    'md5sum',
                    dir_entry.name,
                ],
                    cwd=backup_dir,
                    stdout=subprocess.PIPE
                )

                upload_result_code = 0
                start_upload_time = time.time()
                try:
                    s3_client.upload_file(dir_entry.path, aws_conf['bucket_name'], dir_entry.name)
                except Exception as e:
                    # logger.exception(e)
                    logger.error(e)
                    upload_result_code = 1
                else:
                    logger.info(f'Remove: {dir_entry.path}')
                    os.remove(dir_entry.path)
                upload_duration = int(time.time() - start_upload_time)

                md5sum_output = [l.decode() for l in process.communicate() if l]
                md5sum = ''.join(md5sum_output)
                logger.info(f'Uploading md5sum "{md5sum}')
                s3_client.put_object(
                    Body=md5sum,
                    Bucket=aws_conf['bucket_name'],
                    Key=f'{dir_entry.name}.md5sum.txt'
                )

                if zab_conf:
                    send_to_zabbix(key='backup_file_size',
                                   value=backup_file_size,
                                   config_file=zab_conf['config_file'])
                    send_to_zabbix(key='upload_result_code',
                                   value=upload_result_code,
                                   config_file=zab_conf['config_file'])
                    send_to_zabbix(key='upload_duration',
                                   value=upload_duration,
                                   config_file=zab_conf['config_file'])

                logger.info('Uploaded')


if __name__ == '__main__':
    main()
