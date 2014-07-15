# -*- coding: utf-8 -*-
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
    '\$http_referer': '(?P<http_referer>[\d\D]+|)',  #referer cAN by empty
    '\$http_user_agent': '(?P<http_user_agent>[\d\D]+|)',  #agent can be empty
    #'\$http_x_forwarded_for': '(?P<http_x_forwarded_for>[\d\D.,\-\s]+)',
    '\$http_x_forwarded_for': '(?P<http_x_forwarded_for>[\d\D]+|)',
    '\$msec': '(?P<msec>\d+)',  #время в секундах с точностью до миллисекунд на момент записи в лог
    '\$pipe': '(?P<pipe>[.p])',  #“p” если запрос был pipelined, иначе “.”
    '\$remote_addr': '(?P<remote_addr>\d+.\d+.\d+.\d+)',
    '\$remote_user': '(?P<remote_user>[\D\d]+)',
    '\$request': '(?P<request_request_method>[A-Z]+) (?P<request_request_uri>[\d\D]+) (?P<request_request_http_version>HTTP/[0-9.]+)',
    '\$request_length': '(?P<request_length>\d+)',  #длина запроса (включая строку запроса, заголовок и тело запроса)
    '\$request_time': '(?P<request_time>[\d.]+)',
    #время обработки запроса в секундах с точностью до миллисекунд; время, прошедшее с момента чтения первых байт от клиента до момента записи в лог после отправки последних байт клиенту
    '\$status': '(?P<status>\d+)',  #статус ответа
    '\$time_iso8601': 'NOT_IMPLEMENTED',  #локальное время в формате по стандарту ISO 8601
    '\$time_local': '(?P<time_local>[0-3][0-9]/[A-Za-z]{3}/[0-9]{4}:[0-9]{2}:[0-5][0-9]:[0-5][0-9] [-+0-9]+)',
    #локальное время в Common Log Format
    '\$upstream_addr': '(?P<upstream_addr>[\-A-Za-z0-9.:, ]+)',  #ip:port and unix:socket-path
    '\$upstream_response_time': '(?P<upstream_response_time>[-.\d,: ]+)',
    '\$upstream_status': '(?P<upstream_status>[\-0-9,: ]+)',
    '\$uid_got': '(?P<uid_got>[\-0-9A-Za-z=]+)',
    '\$uid_set': '(?P<uid_set>[\-0-9A-Za-z=]+)',
    '\$abCookieValue': '(?P<ab_cookie_value>[A|B])',
}

