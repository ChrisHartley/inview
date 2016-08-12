import json
import os
import pprint
import argparse
import sqlite3
import psycopg2
from datetime import datetime

class FileNotFound(Exception):
    pass

parser = argparse.ArgumentParser(description='Pretty Print JSON and/or write to db')
parser.add_argument('-f','--file', help='json file', required=True)
parser.add_argument('-d','--database', help='write to database file', required=False)
parser.add_argument('-p','--pprint', help='pretty print json', action="store_true", required=False)

args = parser.parse_args()

f = open(args.file, 'r')

read_file = f.read()
pp = pprint.PrettyPrinter(indent=2)
#pp.pprint(read_file)
json_data = json.loads(read_file)

#pp.pprint(json_data)

if (args.pprint):
    for i in json_data['features']:
        pp.pprint(i['attributes'])
if (args.database):
    conn_string = "host='localhost' dbname='gis' user='chris' password='chris'"
    db = psycopg2.connect(conn_string)
#    db = sqlite3.connect(args.database, isolation_level='None')
    cursor = db.cursor()
    try:
        cursor.execute('''
            create table if not exists incidents(
                agency text,
                agencyonly text,
                aptcondoid text,
                apt_complex_name text,
                beat text,
                cad_number text,
                case_mgmt_link text,
                case_number text,
                city text,
                community text,
                cvwebdate text,
                cvwebtime text,
                dateimport text,
                datetrans text,
                district text,
                generalized_address text,
                grid_id integer,
                impd_beat text,
                incidentreports text,
                incident_code integer,
                incident_code_desc text,
                incident_status text,
                indyneighborhood text,
                iwaddress text,
                iwaddress_link text,
                iwbeat_join text,
                iwdow integer,
                iwfrom_date text,
                iwfrom_time text,
                iwgeoname text,
                iwmarion_join text,
                iwreport_date text,
                iwreport_time text,
                iwto_date text,
                iwto_time  text,
                iwunmatchable_join text,
                jurisdiction text,
                latitude text,
                longitude text,
                mastrel integer,
                nibrs_code text,
                nibrs_desc text,
                nibrs_group text,
                objectid integer,
                officer integer,
                officer2 text,
                report_time text,
                rstrsecgrp text,
                score integer,
                seclevel integer,
                side text,
                source text,
                status text,
                sub_agency text,
                supplement text,
                timetrans text,
                weapon text,
                zip integer
            )
           ''')
        db.commit()
    except Exception as e:
    # Roll back any change if something goes wrong
        db.rollback()
        raise e
    cursor.execute('BEGIN')
    for i in json_data['features']:
        try:
            cvwebdate = datetime.strptime(str(i['attributes']['CVWEBDATE']), '%Y%M%d')
        except:
            cvwebdate = None
        try:
            iwfrom_date = datetime.strptime(str(i['attributes']['IWFROM_DATE']), '%Y%M%d')
        except:
            iwfrom_date = None
        try:
            iwreport_date = datetime.strptime(str(i['attributes']['IWREPORT_DATE']), '%Y%M%d')
        except:
            iwreport_date = None

#        if (i['attributes']['IWTO_DATE'] == 0 or i['attributes']['IWTO_TIME'] == ' '):
        record = (i['attributes']['AGENCY'],i['attributes']['AGENCYONLY'], i['attributes']['APTCONDOID'], i['attributes']['APT_COMPLEX_NAME'], i['attributes']['BEAT'], i['attributes']['CAD_NUMBER'], i['attributes']['CASE_MGMT_LINK'], i['attributes']['CASE_NUMBER'], i['attributes']['CITY'], i['attributes']['COMMUNITY'], cvwebdate, i['attributes']['CVWEBTIME'], i['attributes']['DATEIMPORT'], i['attributes']['DATETRANS'], i['attributes']['DISTRICT'], i['attributes']['GENERALIZED_ADDRESS'], i['attributes']['GRID_ID'], i['attributes']['IMPD_BEAT'], i['attributes']['INCIDENTREPORTS'], i['attributes']['INCIDENT_CODE'], i['attributes']['INCIDENT_CODE_DESC'], i['attributes']['INCIDENT_STATUS'], i['attributes']['INDYNEIGHBORHOOD'], i['attributes']['IWADDRESS'], i['attributes']['IWADDRESS_LINK'], i['attributes']['IWBEAT_JOIN'], i['attributes']['IWDOW'], iwfrom_date, i['attributes']['IWFROM_TIME'], i['attributes']['IWGEONAME'], i['attributes']['IWMARION_JOIN'], iwreport_date, i['attributes']['IWREPORT_TIME'], None, None, i['attributes']['IWUNMATCHABLE_JOIN'], i['attributes']['JURISDICTION'], i['attributes']['LATITUDE'], i['attributes']['LONGITUDE'], i['attributes']['MASTREL'], i['attributes']['NIBRS_CODE'], i['attributes']['NIBRS_DESC'], i['attributes']['NIBRS_GROUP'], i['attributes']['OBJECTID'], i['attributes']['OFFICER'], i['attributes']['OFFICER2'], i['attributes']['REPORT_TIME'], i['attributes']['RSTRSECGRP'], i['attributes']['SCORE'], i['attributes']['SECLEVEL'], i['attributes']['SIDE'], i['attributes']['SOURCE'], i['attributes']['STATUS'], i['attributes']['SUB_AGENCY'], i['attributes']['SUPPLEMENT'], i['attributes']['TIMETRANS'], i['attributes']['WEAPON'], i['attributes']['ZIP'])

        cursor.execute('''insert into incidents (agency, agencyonly, aptcondoid, apt_complex_name, beat, cad_number, case_mgmt_link, case_number, city, community, cvwebdate, cvwebtime, dateimport, datetrans, district, generalized_address, grid_id, impd_beat, incidentreports, incident_code, incident_code_desc, incident_status, indyneighborhood, iwaddress, iwaddress_link, iwbeat_join, iwdow, iwfrom_date, iwfrom_time, iwgeoname, iwmarion_join, iwreport_date, iwreport_time, iwto_date, iwto_time , iwunmatchable_join, jurisdiction, latitude, longitude, mastrel, nibrs_code, nibrs_desc, nibrs_group, objectid, officer, officer2, report_time, rstrsecgrp, score, seclevel, side, source, status, sub_agency, supplement, timetrans, weapon, zip) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',record)

    db.commit()
