#!/usr/bin/python -u
# coding=utf-8

import Queue
import threading
import re
import string
import MySQLdb
import time
import datetime
from BeautifulSoup import BeautifulSoup
import sys, os, getopt
import myToolbox
import codecs
from langconv import Converter

global LOCAL_DB
global LOCAL_CURSOR
global RUN_IN_BG

def store_code_info (lang, code_info):
	global LOCAL_DB
	global LOCAL_CURSOR

	name_zh = Converter ('zh-hant').convert(code_info['name'])

	if lang == 'en' and code_info['division_id'] != 0:
		pass
	elif lang == 'zh' and code_info['division_id'] != 0:
		sql = """INSERT INTO api_country_divisions (division_id, locale, name, adm_code)
	VALUES (%s, %s, %s, %s) ON DUPLICATE KEY UPDATE name=%s"""
		params = (code_info['division_id'], 'zh_CN', code_info['name'], code_info['adm_code'], code_info['name'])
		LOCAL_CURSOR.execute (sql, params)

		sql = """INSERT INTO api_country_division_localized_names (division_id, locale, localized_name)
	VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE localized_name=%s"""
		params = (code_info['division_id'], 'zh_CN', code_info['name'], code_info['name'])
		LOCAL_CURSOR.execute (sql, params)

		sql = """INSERT INTO api_country_division_localized_names (division_id, locale, localized_name)
	VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE localized_name=%s"""
		params = (code_info['division_id'], 'zh', name_zh, name_zh)
		LOCAL_CURSOR.execute (sql, params)

	print ("INFO (%s) > Got a division: 0x%X, %s, %s (%s)" % (myToolbox.get_time (),
			code_info['division_id'], code_info['adm_code'], code_info['name'], name_zh))

def parse_lines (lang, lines):
	id_base = 156
	stop = len(lines) / 2
	for i in range(0, stop, 1):
		code_info = {}
		code_info ['division_id'] = 0

		code = lines[i*2].strip()
		name = lines[i*2 + 1].strip()

		if len (code) < 6:
			print ("WARNING (%s) > Bad code: %s" % (myToolbox.get_time (), code))
			continue

		if code[4:6] == '00':
			if code[2:4] == '00':
				code_info ['division_id'] = id_base * 1024 + int (code[0:2])
			else:
				code_info ['division_id'] = id_base * 1024 * 256 + int (code[0:2]) * 256 + int (code[2:4])
		else:
			code_info ['division_id'] = id_base * 1024 * 256 * 256 + int (code[0:2]) * 256 * 256 + int (code[2:4]) * 256 + int (code[4:6])

		code_info ['name'] = name
		code_info ['adm_code'] = 'CN-' + code

		store_code_info (lang, code_info)

def main (lang, file_name):
	global LOCAL_DB
	global LOCAL_CURSOR

	start = time.time()

	try:
		fd = codecs.open (file_name, "r", "utf-8")
		lines = fd.readlines ()
	except Exception as e:
		print ("ERROR (%s) > Error when read lines from %s (%s)" % (myToolbox.get_time (), file_name, e))
	finally:
		fd.close ()

	if len (lines) > 0:
		LOCAL_DB = MySQLdb.connect (host="ldb", user="fsen_dev", passwd="db4FSEN-DEV@FMSoft0126", db="fsen_dev",
				charset="utf8")
		LOCAL_CURSOR = LOCAL_DB.cursor ()

		parse_lines (lang, lines)
	else:
		sys.exit (0)

	LOCAL_CURSOR.close ()
	LOCAL_DB.commit ()

	print "Elapsed Time: %s" % (time.time() - start)
	sys.exit (0)

def usage ():
	print "./parse_cn_divisions.py [--help] [--file=<path_to_division_file>] [--lang=<en|zh|ja|...>]"

lang = "zh"
file_name = "/data/cached-pages/cn-divisions.txt"
opts, args = getopt.getopt (sys.argv [1:], "hf:l:", ["help", "file=", "lang="])
for op, value in opts:
	if op == "-h" or op == "--help":
		usage ()
		sys.exit (0)
	elif op == "-l" or op == "--lang":
		lang = value
	elif op == "-f" or op == "--file":
		file_name = value

main (lang, file_name)

