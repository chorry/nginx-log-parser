# -*- coding: utf-8 -*-
from __future__ import division

import sys
import time
from pprint import pprint
import signal

from logParser import LogParser


def signal_handler(signal, frame):
    print('Aborted by user.')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

swapFileName = 'worker.swap'
defaultConfig = {
    'offset': 0,
    'linenum': 0,
    'path': None
}
verboseMode = False

defaultNginxLogFormat = ('$remote_addr $host $remote_user [$time_local] $request '
                         '"$status" $body_bytes_sent "$http_referer" '
                         '"$http_user_agent" "$http_x_forwarded_for" '
                         'upstream{$upstream_addr|$upstream_response_time|$upstream_status} $abCookieValue')

upstream_ab36 = ('$remote_addr $host $remote_user [$time_local] $request '
                '"$status" $body_bytes_sent "$http_referer" '
                '"$http_user_agent" "$http_x_forwarded_for" '
                'upstream{$upstream_addr|$upstream_response_time|$upstream_status} $cookie_ab_v6_version $abv3_6 $ab_var "$uid_got|$uid_set"'
)

if __name__ == '__main__':
    #restore settings
    configFileName = "worker.config.json"
    nginxLogFormat = upstream_ab36

    config = {
        'path': './',
        'currentFile': None,
        'resumeOnFile': True,
        'offset': 0
    }

    for arg in sys.argv:
        params = arg.split("=")

        if params[0] == '-path':
            config['path'] = params[1]
        if params[0] == '-file':
            config['currentFile'] = params[1]
        if params[0] == '-nginx':
            config['nginxLogFormat'] = params[1]
        if params[0] == '-verbose':
            config['verboseMode'] = True

    if config['currentFile'] is None:
        print ("Config file is not specified!\n")
        print ("Use -file=FILENAME and -path=PATHNAME")
        exit(0)

    lParser = LogParser(configFileName=configFileName, configParams=config)

    #if you have custom fields, describe regex for them here
    lParser.updateLogParams(
        {'\$cookie_ab_v6_version': '(?P<cookie_ab_v6_version>(v\d|-))',
         '\$abv3_6': '(?P<abv3_6>(v\d|-))',
         '\$ab_var': '(?P<ab_var>(v\d|-))'
        }
    )


    lParser.setLogPattern(nginxLogFormat)

    for processedObj in lParser.parseFile([config['path'] + config['currentFile']]):
        if processedObj is not None:
            pprint(processedObj)

    print("the end")