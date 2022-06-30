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
                if fields['response_order']:
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, response_order:{fields['response_order']} \n")
                if fields['result_status']:
                    if trade2[0]['state'] == 'giveup' or trade2[0]['state'] == 'cancelled':
                        if not fields['result_status'] == 8:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, result_status:{fields['result_status']} \n")
                    if trade2[0]['state'] == 'winner':
                        if not fields['result_status'] == 4:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, result_status:{fields['result_status']} \n")
                    if trade2[0]['state'] == 'finished':
                        if not fields['result_status'] == 16:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, result_status:{fields['result_status']} \n")
                trade7 = firms(trade1['organizer_id'])
                trade6 = organization_data(trade7[0]['code_ogrn'])
                with open("firms.txt", "a") as f:
                    if not trade6[0]['ogrn'] == trade7[0]['code_ogrn']:
                        f.write(
                            f"organizer_id : {trade1['organizer_id']}, ogrn: {trade6[0]['ogrn']} != code_ogrn: {trade7[0]['code_ogrn']}\n")
                    if not trade6[0]['okato_region'] == trade7[0]['post_region']:
                        f.write(
                            f"organizer_id : {trade1['organizer_id']}, okato_region: {trade6[0]['okato_region']} != post_region: {trade7[0]['post_region']}\n")
                    if not trade6[0]['address_post'] == trade7[0]['post_region']:
                        f.write(
                            f"organizer_id : {trade1['organizer_id']}, address_post: {trade6[0]['address_post']} != post_country: {str(trade7[0]['post_country']) + ' ' + trade7[0]['post_index'] + ' ' + trade7[0]['post_region'] + ' ' + trade7[0]['post_town'] + ' ' + trade7[0]['post_address']}\n")
                    if not trade6[0]['address_legal'] == trade7[0]['jury_country']:
                        f.write(
                            f"organizer_id : {trade1['organizer_id']}, address_legal: {trade6[0]['address_legal']} != jury_country: {str(trade7[0]['jury_country']) + ' ' + trade7[0]['jury_index'] + ' ' + trade7[0]['jury_region'] + ' ' + trade7[0]['jury_town'] + ' ' + trade7[0]['jury_address']}\n")
                trade4_pos = positions(trade1["procedure_id"], trade1["lot_id"])
                trade5_lotpos = atom_positions(connection, trade1["lot_id"])
                with open("positions.txt", "a") as f:
                    if trade4_pos:
                        if not trade4_pos[0]['number'] == trade5_lotpos[0]['sequence_number']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, number: {trade4_pos[0]['number']} != sequence_number: {trade5_lotpos[0]['sequence_number']}\n")
                        if not trade4_pos[0]['name'] == trade5_lotpos[0]['name']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, name: {trade4_pos[0]['name']} != name: {trade5_lotpos[0]['name']}\n")
                        if trade4_pos[0].get('customer_name', None):
                            if not trade4_pos[0]['customer_name'] == trade5_lotpos[0]['customer_name']:
                                f.write(
                                    f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, customer_name: {trade4_pos[0]['customer_name']} != customer_name: {trade5_lotpos[0]['customer_name']}\n")
                        if not trade4_pos[0]['quantity'] == trade5_lotpos[0]['quantity']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, quantity: {trade4_pos[0]['quantity']} != quantity: {trade5_lotpos[0]['quantity']}\n")
                        if not trade4_pos[0]['unit'] == trade5_lotpos[0]['units']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, unit: {trade4_pos[0]['unit']} != units: {trade5_lotpos[0]['units']}\n")
                        if not trade4_pos[0]['material_group'] == trade5_lotpos[0]['material_group']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, material_group: {trade4_pos[0]['material_group']} != material_group: {trade5_lotpos[0]['material_group']}\n")
                        if not trade4_pos[0]['nds'] == trade5_lotpos[0]['unit_price_with_vat']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, nds: {trade4_pos[0]['nds']} != unit_price_with_vat: {trade5_lotpos[0]['unit_price_with_vat']}\n")
                        if not trade4_pos[0]['price_with_nds'] == trade5_lotpos[0]['unit_price_with_vat']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, price_with_nds: {trade4_pos[0]['price_with_nds']} != unit_price_with_vat: {trade5_lotpos[0]['unit_price_with_vat']}\n")
                        if not trade4_pos[0]['delivery_start'] == trade5_lotpos[0]['delivery_start_date']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, delivery_start: {trade4_pos[0]['delivery_start']} != delivery_start_date: {trade5_lotpos[0]['delivery_start_date']}\n")
                        if not trade4_pos[0]['delivery_end'] == trade5_lotpos[0]['delivery_finish_date']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, delivery_end: {trade4_pos[0]['delivery_end']} != delivery_finish_date: {trade5_lotpos[0]['delivery_finish_date']}\n")
                        if not trade4_pos[0]['incoterms']['code_incoterms'] == trade5_lotpos[0]['incoterms']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, lot_id: {trade1['lot_id']}, code_incoterms: {trade4_pos[0]['code_incoterms']} != incoterms: {trade5_lotpos[0]['incoterms']}\n")
                trade8 = documentation(trade1["procedure_id"])
                trade9 = documentations(trade1["procedure_id"])
                with open("docs.txt", "a") as f:
                    if trade8:
                        if not trade8[0]['procedure_id'] == trade9[0]['procedure_id']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, procedure_id: {trade8[0]['procedure_id']} != procedure_id: {trade9[0]['procedure_id']}\n")
                        if not trade8[0]['lot_id'] == trade9[0]['lot_id']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, lot_id: {trade8[0]['lot_id']} != lot_id: {trade9[0]['lot_id']}\n")
                        if not trade8[0]['doc_id'] == trade9[0]['document_id']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, doc_id: {trade8[0]['doc_id']} != document_id: {'atom_auction-' + str(trade9[0]['document_id'])}\n")
                        if not trade8[0]['doc_description'] == trade9[0]['description']:
                            f.write(
                                f"procedure_id : {trade1['procedure_id']}, doc_description: {trade8[0]['doc_description']} != description: {trade9[0]['description']}\n")

