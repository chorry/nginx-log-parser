# -*- coding: utf-8 -*-

import re
import time
from pprint import pprint

""" These symbols in nginx_log_format may break regex, if are left unattended"""
LOG_SPECIAL_SYMBOLS = {
    '\[': '\\[',
    '\]': '\\]',
    '\"': '\\"',
    '\|': '\\|',
}

""" Recognizable nginx_log_format parameters"""
LOG_PARAMS = {
    '\$body_bytes_sent': '(?P<body_bytes_sent>\d+)',
    '\$bytes_sent': '(?P<bytes_sent>\d+)',  #число байт, переданное клиенту
    '\$connection': 'NOT_IMPLEMENTED',  #порядковый номер соединения
    '\$connection_requests': '(?P<connection_requests>\d+)',  #текущее число запросов в соединении (1.1.18)
    '\$host': '(?P<host>[A-Za-z0-9-\.]+)',
    '\$http_referer': '(?P<http_referer>[\d\D]+)',
    '\$http_user_agent': '(?P<http_user_agent>[\d\D]+)',
    #'\$http_x_forwarded_for': '(?P<http_x_forwarded_for>[\d\D.,\-\s]+)',
    '\$http_x_forwarded_for': '(?P<http_x_forwarded_for>[\d\D]+|)',
    '\$msec': '(?P<msec>\d+)',  #время в секундах с точностью до миллисекунд на момент записи в лог
    '\$pipe': '(?P<pipe>[.p])',  #“p” если запрос был pipelined, иначе “.”
    '\$remote_addr': '(?P<remote_addr>\d+.\d+.\d+.\d+)',
    '\$remote_user': '(?P<remote_user>\W)',
    '\$request': '(?P<request>[A-Z]{3,4} [\d\D]+ HTTP/[0-9.]+)',
    '\$request_length': '(?P<request_length>\d+)',  #длина запроса (включая строку запроса, заголовок и тело запроса)
    '\$request_time': '(?P<request_time>[\d.]+)',
    #время обработки запроса в секундах с точностью до миллисекунд; время, прошедшее с момента чтения первых байт от клиента до момента записи в лог после отправки последних байт клиенту
    '\$status': '(?P<status>\d+)',  #статус ответа
    '\$time_iso8601': 'NOT_IMPLEMENTED',  #локальное время в формате по стандарту ISO 8601
    '\$time_local': '(?P<time_local>[0-3][0-9]/[A-Za-z]{3}/[0-9]{4}:[0-2]{2}:[0-5][0-9]:[0-5][0-9] [-+0-9]+)',
    #локальное время в Common Log Format
    '\$upstream_addr': '(?P<upstream_addr>[\-A-Za-z0-9.:, ]+)',  #ip:port and unix:socket-path
    '\$upstream_response_time': '(?P<upstream_response_time>[-.\d: ]+)',
    '\$upstream_status': '(?P<upstream_status>[-0-9: ]+)',
}


def dict_sub(text, d=LOG_PARAMS):
    """ Replace in 'text' non-overlapping occurences of REs whose patterns are keys
    in dictionary 'd' by corresponding values (which must be constant strings: may
    have named backreferences but not numeric ones). The keys must not contain
    anonymous matching-groups.
    Returns the new string.
    Thanks to Alex Martelli @
    http://stackoverflow.com/questions/937697/can-you-pass-a-dictionary-when-replacing-strings-in-python
    """
    if d != LOG_SPECIAL_SYMBOLS:
        text = dict_sub(text, LOG_SPECIAL_SYMBOLS)


    # Create a regular expression  from the dictionary keys
    regex = re.compile("|".join("(%s)" % k for k in d))
    # Facilitate lookup from group number to value
    lookup = dict((i + 1, v) for i, v in enumerate(d.itervalues()))
    # For each match, find which group matched and expand its value
    result = regex.sub(lambda mo: mo.expand(lookup[mo.lastindex]), text)

    return result


def parseLogLine(compiledReObject, text, displayResult=True):
    result = compiledReObject.finditer(text)
    group_name_by_index = dict([(v, k) for k, v in compiledReObject.groupindex.items()])

    iteratorSize = 0
    for match in result:
        iteratorSize += 1
        for group_index, group in enumerate(match.groups()):
            if group and displayResult:
                #print group_index, group
                print "%s : %s" % (group_name_by_index[group_index + 1], group)
                #time.sleep(1)
    if iteratorSize == 0:
        print "Could not parse [ %s ]" % text

    return iteratorSize


if __name__ == '__main__':
    nginxLogFormat = ('$remote_addr $host $remote_user [$time_local] $request '
                      '"$status" $body_bytes_sent "$http_referer" '
                      '"$http_user_agent" "$http_x_forwarded_for" '
                      'upstream{$upstream_addr|$upstream_response_time|$upstream_status}')

    rePattern = dict_sub(nginxLogFormat)

    object = re.compile(rePattern)

    logFileName = 'custom_short.log'

    linenum = 0

    #parseLogLine(compiledReObject=object, text=myString)

    with open(logFileName) as f:
        for line in f:
            linenum += 1
            parseLogLine(compiledReObject=object, text=line, displayResult=False)

    print "the end"