import pymysql.cursors
import json
import datetime

HOST_NAME_33 = '10.31.82.33'
USER_NAME_33 = 'services'
USER_PASSWORD_33 = 'Cdte8cVr7RhHQWdq'

HOST_NAME_200 = '10.31.81.200'
USER_NAME_200 = 'fabrikant'
USER_PASSWORD_200 = 'bd6f1e86'

ts = '2022-01-20 13:00:00'

connection = pymysql.connect(user=USER_NAME_33, password=USER_PASSWORD_33, host=HOST_NAME_33, database='procedure_list',
                             cursorclass=pymysql.cursors.DictCursor)

lots_view_info_id = []
lots_view_info_id = tuple(int(i) for i in lots_view_info_id)

with connection:
    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT organizer_id, date_publication, date_end, date_partaking_to, date_unsealing, procedure_id, lot_id, date_updated, fields FROM lots_view_info WHERE date_updated > %s AND lots_view_info_id in %s"
        cursor.execute(sql, (ts, lots_view_info_id))
        procedures_search = [row for row in cursor]


def procedures(procedure_id):
    connection = pymysql.connect(user=USER_NAME_200, password=USER_PASSWORD_200, host=HOST_NAME_200,
                                 database='fabrikant_atom_price_monitoring',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            # Read a single record
            print(procedure_id)
            sql = "SELECT * FROM procedures WHERE procedure_id = %s"
            cursor.execute(sql, (procedure_id))
            return [row for row in cursor]


def positions(procedure_id, lot_id):
    connection = pymysql.connect(user=USER_NAME_33, password=USER_PASSWORD_33, host=HOST_NAME_33,
                                 database='procedure_list',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM positions WHERE procedure_id = %s and lot_id = %s"
            cursor.execute(sql, (procedure_id, lot_id))
            return [row for row in cursor]


def atom_positions(connection, procedure_id):
    connection = pymysql.connect(user=USER_NAME_200, password=USER_PASSWORD_200, host=HOST_NAME_200,
                                 database='fabrikant_atom_price_monitoring',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT * positions WHERE procedure_id = %s"
            cursor.execute(sql, (procedure_id))
            return [row for row in cursor]


def firms(id):
    connection = pymysql.connect(user=USER_NAME_200, password=USER_PASSWORD_200, host=HOST_NAME_200,
                                 database='fabrikant_test',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM firms WHERE id = %s"
            cursor.execute(sql, (id))
            return [row for row in cursor]


def organization_data(ogrn):
    connection = pymysql.connect(user=USER_NAME_33, password=USER_PASSWORD_33, host=HOST_NAME_33,
                                 database='procedure_list',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM organization_data WHERE ogrn = %s"
            cursor.execute(sql, (ogrn))
            return [row for row in cursor]


def documentation(procedure_id):
    connection = pymysql.connect(user=USER_NAME_33, password=USER_PASSWORD_33, host=HOST_NAME_33,
                                 database='procedure_list',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM documentation WHERE procedure_id = %s"
            cursor.execute(sql, (procedure_id))
            return [row for row in cursor]


def documentations(procedure_id):
    connection = pymysql.connect(user=USER_NAME_200, password=USER_PASSWORD_200, host=HOST_NAME_200,
                                 database='fabrikant_atom_price_monitoring',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM documentations WHERE procedure_id = %s"
            cursor.execute(sql, (procedure_id))
            return [row for row in cursor]


for trade1 in procedures_search:
    trade2 = procedures(trade1["procedure_id"])
    fields = json.loads(trade1['fields'])

    with open("bad_field.txt", "a") as f:
        if trade2:
            if isinstance(fields['dates'], dict):
                if not (fields['dates']['date_publication']['value']['current']) == str(trade2[0]['date_publication']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, date_publication:{fields['dates']['date_publication']['value']['current']} != date_publication:{trade2[0]['date_publication']}\n")
                if not (fields['dates']['date_end']['value']['current']) == str(trade2[0]['date_publication']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, date_end:{fields['dates']['date_end']['value']['current']} != date_end:{trade2[0]['date_end']}\n")
                if fields['comments']:
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, comments:{fields['comments']} \n")
