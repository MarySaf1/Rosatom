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
lots_view_info_id = ["449099399","449293344","449293349","449293350","449293148","449296248","449316927","449316936",
                     "449320203","449337518","449337800","449337825","449338633","449338607","449338644","449338651",
                     "449338760","449338786","449335542","449339784","449339825","449339834","449339983","449340051",
                     "449340059","449340079","449340150","449340173","449340220","449340624","449340810","449283691",
                     "449344596","449344679","449344702","449344780","449349233","449353037","449318461","449330774",
                     "449355961","449358635","449359645","449359682","449360032","449187775","449374890","449415801",
                     "449420308","449420606","449420645","449420659","449420713","449353745","449354476","449356305",
                     "449399262","449442319","449448667","449448698","449448727","449448743","449448754","449448770",
                     "449491085","449448819","449491592","449491611","449491625","449491105","449497566","449497708",
                     "449497713","449497775","449507971","449537244","449531075","449491658","449568666","449568673",
                     "449569274","449578247","449587804","449587901","449584933","440005390","449602073","449614128",
                     "449619138","449644803","449645895","449651549","449651552","449651561","449656479","449657662",
                     "449657951","449664233","449664234","440145921"]
lots_view_info_id = tuple(int(i) for i in lots_view_info_id)

with connection:
    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT organizer_id, date_publication, date_end, date_partaking_to, date_unsealing, procedure_id, lot_id, date_updated, fields FROM lots_view_info WHERE date_updated > %s AND lots_view_info_id in %s"
        cursor.execute(sql, (ts, lots_view_info_id))
        procedures_search = [row for row in cursor]


def lots(procedure_id, lot_id):
    connection = pymysql.connect(user=USER_NAME_200, password=USER_PASSWORD_200, host=HOST_NAME_200,
                                 database='fabrikant_atom_auction',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            # Read a single record
            print(procedure_id)
            print(lot_id)
            sql = "SELECT * FROM lots WHERE procedure_id = %s AND lot_id = %s"
            cursor.execute(sql, (procedure_id, lot_id))
            return [row for row in cursor]


def lots_info(connection, lot_id):
    connection = pymysql.connect(user=USER_NAME_200, password=USER_PASSWORD_200, host=HOST_NAME_200,
                                 database='fabrikant_atom_auction',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM lots_info WHERE lot_id = %s"
            cursor.execute(sql, (lot_id))
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


def lot_positions(connection, lot_id):
    connection = pymysql.connect(user=USER_NAME_200, password=USER_PASSWORD_200, host=HOST_NAME_200,
                                 database='fabrikant_atom_auction',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM lot_positions WHERE lot_id = %s"
            cursor.execute(sql, (lot_id))
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
                                 database='fabrikant_atom_auction',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM documentations WHERE procedure_id = %s"
            cursor.execute(sql, (procedure_id))
            return [row for row in cursor]


def fabrikant_storage(file_id):
    connection = pymysql.connect(user=USER_NAME_200, password=USER_PASSWORD_200, host=HOST_NAME_200,
                                 database='fabrikant_storage',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT name FROM storage WHERE file_id = %s"
            cursor.execute(sql, (file_id))
            return [row for row in cursor]

for trade1 in procedures_search:
    trade2 = lots(trade1["procedure_id"], trade1["lot_id"])
    fields = json.loads(trade1['fields'])

    with open("bad_field.txt", "a") as f:
        if trade2:
            if isinstance(fields['dates'], dict):
                if not (fields['dates']['date_publication']['value']['current']) == str(trade2[0]['date_publication']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, date_publication:{fields['dates']['date_publication']['value']['current']} != date_publication:{trade2[0]['date_publication']}\n")
