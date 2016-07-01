#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import boto3
import datetime
import logging
import os
import re
import signal
import SimpleHTTPServer
import SocketServer
import subprocess
import sys
import tarfile
import yaml
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def exclude_filter(tinfo, exclude_list):
    for exclude in exclude_list:
        if re.match(exclude, tinfo.name):
            return None
    return tinfo


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--port', default=8000, type=int)
    parser.add_argument('--config', default='config.yaml')
    return parser.parse_args()


class S3Volume(object):
    def __init__(self, bucket_name, config_file='config.yaml'):
        self.logger = logging.getLogger(self.__class__.__name__)
        s3 = boto3.resource('s3')
        self.bucket = s3.Bucket(bucket_name)
        objs = list(self.bucket.objects.filter(Prefix=config_file))
        if len(objs) == 0:
            raise RuntimeError("Cannot find {0} in the buckt {1}".
                               format(config_file, bucket_name))
        response = self.bucket.Object(config_file).get()
        self.config = yaml.load(response['Body'].read())
        self.tmp_dir = self.config.get('tmp', '/tmp')
        signal.signal(signal.SIGINT, self.signal)
        signal.signal(signal.SIGTERM, self.signal)

    def backup(self):
        suffix = datetime.datetime.now().strftime("-%Y%m%d-%H%M%S") + '.tar.gz'
        for backup in self.config['backups']:
            if 'path' not in backup:
                continue
            path = backup['path']
            backup_file = backup['prefix'] + suffix
            exclude_list = backup.get('exclude', [])
            s3_params = backup.get('s3', {})
            self.logger.info("Start backup: %s to %s", path, backup_file)
            tar_file = os.path.join(self.tmp_dir,
                                    os.path.basename(backup_file))
            tar = tarfile.open(tar_file, 'w:gz')
            tar.add(path, arcname='',
                    filter=lambda x: exclude_filter(x, exclude_list))
            tar.close()
            self.bucket.put_object(Key=backup_file,
                                   Body=open(tar_file, 'rb'),
                                   **s3_params)
            self.logger.info("Done backup: %s", backup.get('name', ''))

    def restore(self):
        for backup in self.config['backups']:
            if 'path' not in backup:
                continue
            path = backup['path']
            self.logger.info("Restoring to {0}".format(path))
            if not os.path.exists(path):
                os.makedirs(path)
                if 'chmod' in backup:
                    self.logger.info("chmod {0}".format(backup['chmod']))
                    subprocess.call(['chmod', backup['chmod'], path])
                if 'chown' in backup:
                    self.logger.info("chown {0}".format(backup['chown']))
                    subprocess.call(['chown', backup['chown'], path])
            backups = sorted(self.bucket.objects.
                             filter(Prefix=backup['prefix']))
            if backups:
                key = backups[-1].key
                self.logger.info("Restoring from {0}".format(key))
                tar_file = os.path.join(self.tmp_dir, os.path.basename(key))
                self.bucket.download_file(key, tar_file)
                tar = tarfile.open(tar_file, 'r:gz')
                tar.extractall(backup['path'])
                tar.close()

    def signal(self, sig, stack):
        self.backup()
        raise SystemExit('Exiting')


class ServerHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def do_POST(self):
        self.log_message('Backup started')
        self.server.backup.backup()
        self.send_response(200)


class Server(SocketServer.TCPServer):
    allow_reuse_address = True

args = get_args()
backup = S3Volume(args.bucket, config_file=args.config)
backup.restore()

Handler = ServerHandler
httpd = Server(("", args.port), Handler)
httpd.backup = backup

logger.info("Server started port:%d", args.port)
try:
    httpd.serve_forever()
finally:
    logger.info("Finished")
