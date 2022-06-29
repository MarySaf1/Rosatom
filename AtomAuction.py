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


for trade1 in procedures_search:
    trade2 = lots(trade1["procedure_id"], trade1["lot_id"])
    fields = json.loads(trade1['fields'])

    with open("bad_field.txt", "a") as f:
        if trade2:
            if not (trade1['date_publication'] == trade2[0]['date_publication']):
                f.write(
                    f"procedure_id : {trade1['procedure_id']}, date_publication:{trade1['date_publication']} != date_publication:{trade2[0]['date_publication']}\n")
            if not (trade1['date_end'] == trade2[0]['date_second_participants_determination']):
                f.write(
                    f"procedure_id : {trade1['procedure_id']}, date_end:{trade1['date_end']} != date_second_participants_determination:{trade2[0]['date_second_participants_determination']}\n")
            if not (trade1['date_partaking_to'] == trade2[0]['date_end_proposal_submission']):
                f.write(
                    f"procedure_id : {trade1['procedure_id']}, date_partaking_to:{trade1['date_partaking_to']} != date_end_proposal_submission:{trade2[0]['date_end_proposal_submission']}\n")
            if not (trade1['date_unsealing'] == trade2[0]['date_end_proposal_submission']):
                f.write(
                    f"procedure_id : {trade1['procedure_id']}, date_unsealing:{trade1['date_unsealing']} != date_end_proposal_submission:{trade2[0]['date_end_proposal_submission']}\n")
            if fields.get('is_lot_trade', None):
                if not (fields['is_lot_trade']) == True:
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, is_lot_trade:{fields['is_lot_trade']} != True\n")
            if fields.get('common_purchase_name'):
                if not (fields['common_purchase_name']) == trade2[0]['common_name']:
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, common_purchase_name:{fields['common_purchase_name']} != common_name:{trade2[0]['common_name']}\n")

            if isinstance(fields['dates'], dict):
                if not (fields['dates']['start_date']['value']['current']) == str(trade2[0]['date_publication']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, start_date:{fields['dates']['start_date']['value']['current']} != date_publication:{trade2[0]['date_publication']}\n")
                if not (fields['dates']['application_end_date']['value']['current']) == str(
                        trade2[0]['date_end_proposal_submission']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, application_end_date:{fields['dates']['application_end_date']['value']['current']} != date_end_proposal_submission:{trade2[0]['date_end_proposal_submission']}\n")

                if not (fields['dates']['unseal_date']['value']['current']) == str(
                        trade2[0]['date_proposal_unsealing']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, unseal_date:{fields['dates']['unseal_date']['value']['current']} != date_proposal_unsealing:{trade2[0]['date_proposal_unsealing']}\n")

                if not (fields['dates']['first_part_date']['value']['current']) == str(
                        trade2[0]['date_first_participants_determination']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, first_part_date:{fields['dates']['first_part_date']['value']['current']} != date_first_participants_determination:{trade2[0]['date_first_participants_determination']}\n")
                if not (fields['dates']['second_part_date']['value']['current']) == str(
                        trade2[0]['date_second_participants_determination']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, second_part_date:{fields['dates']['second_part_date']['value']['current']} != date_second_participants_determination:{trade2[0]['date_second_participants_determination']}\n")
                if not (fields['dates']['evaluation_date']['value']['current']) == str(
                        trade2[0]['date_second_participants_determination']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, evaluation_date:{fields['dates']['evaluation_date']['value']['current']} != date_second_participants_determination:{trade2[0]['date_second_participants_determination']}\n")
                if not (fields['dates']['auction_start_date']['value']['current']) == str(trade2[0]['date_begin']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, auction_start_date:{fields['dates']['auction_start_date']['value']['current']} != date_begin:{trade2[0]['date_begin']}\n")
                if not (fields['dates']['end_date']['value']['current']) == str(
                        trade2[0]['date_second_participants_determination']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, end_date:{fields['dates']['end_date']['value']['current']} != date_second_participants_determination:{trade2[0]['date_second_participants_determination']}\n")
                if not (fields['dates']['summing_up_date']['value']['current']) == str(
                        trade2[0]['date_second_participants_determination']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, summing_up_date:{fields['dates']['summing_up_date']['value']['current']} != date_second_participants_determination:{trade2[0]['date_second_participants_determination']}\n")
                if not (fields['dates']['contract_date']['value']['current']) == str(trade2[0]['date_contract']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, contract_date:{fields['dates']['contract_date']['value']['current']} != date_contract:{trade2[0]['date_contract']}\n")
                if not (fields['dates']['qualification_application_end_date']['value']['current']) == str(trade2[0][
                                                                                                              'date_prequalification_proposals_end']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, qualification_application_end_date:{fields['dates']['qualification_application_end_date']['value']['current']} != date_prequalification_proposals_end:{trade2[0]['date_prequalification_proposals_end']}\n")
                if not (fields['dates']['qualification_review_date']['value']['current']) == str(
                        trade2[0]['date_prequalification_participants_determination']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, qualification_review_date:{fields['dates']['qualification_review_date']['value']['current']} != date_prequalification_participants_determination:{trade2[0]['date_prequalification_participants_determination']}\n")
                if not (fields['dates']['qualification_end_date']['value']['current']) == str(
                        trade2[0]['date_prequalification_end']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, qualification_end_date:{fields['dates']['qualification_end_date']['value']['current']} != date_prequalification_end:{trade2[0]['date_prequalification_end']}\n")
            else:
                f.write(
                    f"procedure_id : {trade1['procedure_id']}, fields['dates'] - Not dict {fields['dates']} != dates:{trade2[0]} \n")
            if isinstance(fields['prices'], dict) and fields['prices']:
                if not (fields['prices']['price']['value']['current']) == str(trade2[0]['real_price']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, price:{fields['prices']['price']['value']['current']} != real_price:{trade2[0]['real_price']}\n")
                if not (fields['prices']['auction_step_min']['value']['current']) == str(trade2[0]['step_min']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, auction_step_min:{fields['prices']['auction_step_min']['value']['current']} != step_min :{trade2[0]['step_min ']}\n")
                if not (fields['prices']['auction_step_max']['value']['current']) == str(trade2[0]['step_max']):
                    f.write(
                        f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, auction_step_max:{fields['prices']['auction_step_max']['value']['current']} != step_max :{trade2[0]['step_max ']}\n")
            else:
                f.write(
                    f"procedure_id : {trade1['procedure_id']}, trading_platform_id: {trade1['trading_platform_id']}, lot_id: {trade1['lot_id']}, section_type: {trade1['section_type']}, price:{fields['prices']} Not dict {fields['prices']} != real_price:{trade2[0]['real_price']}\n")
            trade3 = lots_info(connection, trade1["lot_id"])
            with open("bad_field2.txt", "a") as f:
                if trade3:
                    fields = json.loads(trade1['fields'])
                    if fields.get('comments', None):
                        if not (fields['comments']) == trade3[0]['comments']:
                            f.write(
                                f"lot_id : {trade1['lot_id']}, comments:{fields['comments']} != comments:{trade3[0]['comments']}\n")
                    if fields.get('delivery_condition', None):
                        if not (fields['delivery_condition']) == trade3[0]['delivery_conditions']:
                            f.write(
                                f"lot_id : {trade1['lot_id']}, delivery_condition:{fields['delivery_condition']} != delivery_conditions:{trade3[0]['delivery_conditions']}\n")
                        if not (fields['request_order']) == trade3[0]['delivery_conditions']:
                            f.write(
                                f"lot_id : {trade1['lot_id']}, delivery_condition:{fields['request_order']} != delivery_conditions:{trade3[0]['delivery_conditions']}\n")
                        if not (fields['payment_condition']) == trade3[0]['payment_conditions']:
                            f.write(
                                f"lot_id : {trade1['lot_id']}, delivery_condition:{fields['payment_condition']} != payment_conditions:{trade3[0]['payment_conditions']}\n")
                        if not (fields['customer_requisites']) == trade3[0]['banking_details_customer']:
                            f.write(
                                f"lot_id : {trade1['lot_id']}, customer_requisites:{fields['customer_requisites']} != banking_details_customer:{trade3[0]['banking_details_customer']}\n")
                    if isinstance(fields.get('providing', None), dict):
                        f.write(
                            f"lot_id : {trade1['lot_id']}, offer_request_financing.title: {fields['providing']['offer_request_financing']['title']}\n")
                        if not (fields['providing']['offer_request_financing']['form']) == trade3[0]['form_ensuring']:
                            f.write(
                                f"lot_id : {trade1['lot_id']}, form:{fields['providing']['offer_request_financing']['form']} != form_ensuring:{trade3[0]['form_ensuring']}\n")
                        if not (fields['providing']['offer_request_financing']['size']) == trade3[0][
                            'amount_and_currency_ensuring_request']:
                            f.write(
                                f"lot_id : {trade1['lot_id']}, size:{fields['providing']['offer_request_financing']['size']} != amount_and_currency_ensuring_request:{trade3[0]['amount_and_currency_ensuring_request']}\n")
                        if fields['providing']['offer_request_financing'].get('providing', None):
                            if not (fields['providing']['offer_request_financing']['validity']) == trade3[0]['details_ensuring_request']:
                                f.write(
                                    f"lot_id : {trade1['lot_id']}, validity:{fields['providing']['offer_request_financing']['validity']} != details_ensuring_request:{trade3[0]['details_ensuring_request']}\n")

                        if isinstance(fields['providing'].get('ensuring_return_advance', None), dict):
                            f.write(
                                f"lot_id : {trade1['lot_id']}, ensuring_return_advance.title: {fields['providing']['ensuring_return_advance']['title']}\n")
                            if not (fields['providing']['ensuring_return_advance']['form']) == trade3[0][
                                'form_return_advance']:
                                f.write(
                                    f"lot_id : {trade1['lot_id']}, form:{fields['providing']['ensuring_return_advance']['form']} != form_return_advance:{trade3[0]['form_return_advance']}\n")
                            if not (fields['providing']['ensuring_return_advance']['size']) == trade3[0][
                                'amount_ensuring']:
                                f.write(
                                    f"lot_id : {trade1['lot_id']}, size:{fields['providing']['ensuring_return_advance']['size']} != amount_ensuring:{trade3[0]['amount_ensuring']}\n")
                            if not (fields['providing']['ensuring_return_advance']['requisites']) == trade3[0][
                                'period_ensuring']:
                                f.write(
                                    f"lot_id : {trade1['lot_id']}, requisites:{fields['providing']['ensuring_return_advance']['requisites']} != period_ensuring:{trade3[0]['period_ensuring']}\n")

                        if isinstance(fields['providing'].get('ensuring_obligation_contract', None), dict):
                            f.write(
                                f"lot_id : {trade1['lot_id']}, ensuring_obligation_contract.title: {fields['providing']['ensuring_obligation_contract']['title']}\n")
                            if not (fields['providing']['ensuring_obligation_contract']['form']) == trade3[0][
                                'form_obligation_ensuring']:
                                f.write(
                                    f"lot_id : {trade1['lot_id']}, form:{fields['providing']['ensuring_obligation_contract']['form']} != form_obligation_ensuring:{trade3[0]['form_obligation_ensuring']}\n")
                            if not (fields['providing']['ensuring_obligation_contract']['size']) == trade3[0][
                                'amount_obligation_ensuring']:
                                f.write(
                                    f"lot_id : {trade1['lot_id']}, size:{fields['providing']['ensuring_obligation_contract']['size']} != amount_obligation_ensuring:{trade3[0]['amount_obligation_ensuring']}\n")
                            if not (fields['providing']['ensuring_obligation_contract']['requisites']) == trade3[0][
                                'period_obligation_ensuring']:
                                f.write(
                                    f"lot_id : {trade1['lot_id']}, requisites:{fields['providing']['ensuring_obligation_contract']['requisites']} != period_obligation_ensuring:{trade3[0]['period_obligation_ensuring']}\n")

                        if isinstance(fields['providing'].get('ensuring_guarantee_obligation', None), dict):
                            f.write(
                                f"lot_id : {trade1['lot_id']}, ensuring_guarantee_obligation.title: {fields['providing']['ensuring_guarantee_obligation']['title']}\n")
                            if not (fields['providing']['ensuring_guarantee_obligation']['form']) == trade3[0][
                                'form_guarantee_ensuring']:
                                f.write(
                                    f"lot_id : {trade1['lot_id']}, form:{fields['providing']['ensuring_guarantee_obligation']['form']} != form_guarantee_ensuring:{trade3[0]['form_guarantee_ensuring']}\n")
                            if not (fields['providing']['ensuring_guarantee_obligation']['size']) == trade3[0][
                                'amount_guarantee_ensuring']:
                                f.write(
                                    f"lot_id : {trade1['lot_id']}, size:{fields['providing']['ensuring_guarantee_obligation']['size']} != amount_guarantee_ensuring:{trade3[0]['amount_guarantee_ensuring']}\n")
                            if not (fields['providing']['ensuring_guarantee_obligation']['requisites']) == trade3[0][
                                'period_guarantee_ensuring']:
                                f.write(
                                    f"lot_id : {trade1['lot_id']}, requisites:{fields['providing']['ensuring_guarantee_obligation']['requisites']} != period_guarantee_ensuring:{trade3[0]['period_guarantee_ensuring']}\n")

            trade4_pos = positions(trade1["procedure_id"], trade1["lot_id"])
            trade5_lotpos = lot_positions(connection, trade1["lot_id"])
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
