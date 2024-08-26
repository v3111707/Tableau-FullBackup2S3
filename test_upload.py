import typer
import hashlib
import os
from base64 import b64encode
from typing_extensions import Annotated
from functools import partial
from full_backup2s3 import CONFIG_FILE, get_config, S3Wrapper, init_logger


def checksumSha256(filename):
    """
    returns the hex SHA256 of the file filename
    """
    hash = hashlib.sha256()
    with open(filename, 'rb') as f:
        for b in iter(partial(f.read, 2 ** 10), b''):
            hash.update(b)
    return hash.hexdigest()



app = typer.Typer(
    add_completion=False,
    # pretty_exceptions_enable=False
)


@app.command(context_settings=dict(help_option_names=["-h", "--help"]))
def cli(
        file: Annotated[str, typer.Option('--file', '-f', help='File for download')],
        debug: Annotated[bool, typer.Option('--debug', '-d', help='More logs')] = True,
):
    init_logger('main', debug=debug)
    config = get_config(CONFIG_FILE)
    aws_conf = config['AWS']
    s3_wrapper = S3Wrapper(
        region_name=aws_conf['region_name'],
        aws_access_key_id=aws_conf['aws_access_key_id'],
        aws_secret_access_key=aws_conf['aws_secret_access_key']
    )
    if os.path.isfile(file):
        resp = s3_wrapper.upload_file_with_md5sum(
            file=file,
            bucket=aws_conf['bucket_name'],
            key=file.split('/')[-1],
        )
        print(resp)


if __name__ == "__main__":
    app()
