#!/usr/bin/python
import psycopg2

"""
    Connect to the PostgreSQL database server
"""

def connect():
    conn = None
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect("dbname=ZNO_RESULTS user=postgres password=123456789")
    conn.autocommit = True
    print('Connected')
    return conn

def disconnect(conn):
    conn.close()
    print('Database connection closed.')