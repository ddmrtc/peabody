import logging
import pandas

def checkPandasDiff(pandasOld, pandasNew, excludes = [], key = '', typeId = '', checkRemoved = True):
    # функция для того, что бы сравнить два одинаковых pandas
    # вывести
    #   добавленные строчки
    #   измененные строчки
    #   пропущенные строчки
    #   удаленные строчки
    # функция может исключать столбцы, которые не нужны
    added, changed, skipped, removed = 0, 0, 0, 0
    # сообщения
    messages = []
    # удаляем столбцы, которые нужно исключить
    if excludes:
        exclude = [x for x in excludes if x in pandasOld.columns.tolist()]
        if exclude:
            pandasOld.drop(columns = exclude, inplace = True)
            pandasNew.drop(columns = exclude, inplace = True)
    # проверяем возможно у нас одинаковые pandas
    if pandasOld.equals(pandasNew):
        return False, {'add': added, 'change': changed, 'skip': skipped, 'remove': removed}
    else:
        if key:
            # сравниваем столбцы
            columns     = pandasOld.columns.tolist()
            columnsNew  = pandasNew.columns.tolist()
            logging.debug(f"columns  left: {columns}")
            logging.debug(f"columns right: {columnsNew}")
            # добавляем столбцы, если их нету в одном или другом pandas
            if columns != columnsNew:
                columnsLeft =  [x for x in columns     if x not in columnsNew]
                columnsRight = [x for x in columnsNew  if x not in columns]
                if columnsLeft:
                    logging.debug(f"adding {columnsLeft} to right side (pandasNew)")
                    pandasNew = pandas.concat([pandasNew, pandas.DataFrame(columns=columnsLeft )])
                if columnsRight:
                    logging.debug(f"adding {columnsRight} to left side (pandasOld)")
                    pandasOld = pandas.concat([pandasOld, pandas.DataFrame(columns=columnsRight)])
            # совмещаем по ключу
            diff = pandas.concat([pandasOld.set_index(key), pandasNew.set_index(key)], axis='columns', keys=['First', 'Second'])
            logging.debug(f"{diff}")

            left  = diff['First'].isnull().all(1)
            right = diff['Second'].isnull().all(1)
            # добавлены строчки            
            if not diff[left].empty:
                logging.info(f"ADDED {len(diff[left]['Second'])} {typeId.upper()}:\n{diff[left]['Second']}\n")
                added += len(diff[left]['Second'])
                #email_data['added'] = diff[open]['Second']
                # for i, row in diff[open].iterrows():
                #     message = {'typeId': typeId, 'key': settings['key'], 'id': i, 'action': 'add', 'data': row['Second'].to_dict()}
                #     logging.debug(f"events.put: {message}")
                #     #await events.put(message)
                #     if typeId == 'operations' and message['data']['operationType'] == 'PayOut':
                #         message['data']['id'] = i
                #         payouts.append(message['data'])
                #     added += 1
            # if payouts:
            #     logging.debug(f"PAYOUTS:\n{pandas.DataFrame(payouts).set_index('id')}")
            # удалены строчки
            if not diff[right].empty and checkRemoved:
                logging.info(f"REMOVED {len(diff[right]['First'])} {typeId.upper()}:\n{diff[right]['First']}\n")
                removed += len(diff[right]['First'])
                ##email_data['removed'] = diff[close]['First']
            #     for i, row in diff[close].iterrows():
            #         payouts_skipped_flag = False
            #         message = {'typeId': typeId, 'key': key, 'id': i, 'action': 'remove', 'data': row['First'].to_dict()}
            #         if typeId != 'operations':
            #             logging.debug(f"events.put: {message}")
            #             #await events.put(message)
            #         else:
            #             skipped += 1
            #         if typeId == 'operations' and message['data']['operationType'] == 'PayOut' and payouts:
            #             check = [x for x in payouts if x['payment'] == message['data']['payment'] and x['currency'] == message['data']['currency']]
            #             if check:
            #                 logging.debug(f"{check}")
            #                 if len(check) == 1:
            #                     logging.info(f"FOUND MATCHED ADDED PAYOUT OPERATION: {check[0]['id']}, SKIP REMOVED OPERATION: {i}")
            #                     message['data']['id'] = i
            #                     payouts_skipped.append(message['data'])
            #                     payouts_skipped_flag = True
            #                 else:
            #                     logging.error(f"FOUND {len(check)} MATCHED ADDED PAYOUT OPERATIONS, CAN'T SKIP")
            #         if not payouts_skipped_flag:
            #             removed += 1
            # if skipped:
            #     logging.debug(f"SKIPPED OPERATIONS REMOVE: {skipped}")
            # if payouts_skipped:
            #     #email_data['skipped'] = pandas.DataFrame(payouts_skipped).set_index('id')
            #     #logging.info(f"SKIPPED PAYOUTS:\n{email_data['skipped']}\n")
            #     pass

            # удаляем добавленные/удаленные строчки
            diff = diff.drop(diff[left | right].index)

            columns = [x for x in columns if x != key]
            diff = diff.swaplevel(axis='columns')[columns]
            for column in columns:
                logging.debug(f"checking column: {column}")
                if not diff[column,'First'].equals(diff[column,'Second']):
                    not_equals = diff[column,'First'] != diff[column,'Second']
                    not_nan = pandas.notnull(diff[column,'First']) | pandas.notnull(diff[column,'Second'])
                    diff2 = diff[not_equals & not_nan]
                    for i, row in diff2.iterrows():
                        if column == 'date':
                            if params['deep_debug']:
                                logging.debug(type(row[column,'First']))
                                logging.debug(type(row[column,'Second']))
                            if row[column,'First'][0:19] == row[column,'Second'][0:19]:
                                logging.info(f"CHANGED {typeId.upper():<20} {i:<20} {column:<20} {row[column,'First']:<32} {'->':^5} {row[column,'Second']:<32} DATE CHECK PASSED {row[column,'First'][0:19]}")
                                skipped += 1
                                continue
                            else:
                                logging.info(f"CHANGED {typeId.upper():<20} {i:<20} {column:<20} {row[column,'First']:<32} {'->':^5} {row[column,'Second']:<32} DATE CHECK NOT PASSED")
                                skipped += 1
                                continue
                        elif column == 'commission':
                            if row[column,'First'] == 0.0:
                                logging.info(f"CHANGED {typeId.upper():<20} {i:<20} {column:<20} {row[column,'First']:<32} {'->':^5} {row[column,'Second']:<32} COMISSION CHECK PASSED")
                                skipped += 1
                                continue
                        if type(row[column,'First']) == type([]) or type(row[column,'Second']) == type([]):
                            diff2, diff2_data = checkDiff(row[column,'First'], row[column,'Second'], 'trades')
                            if diff2:
                                added   += diff2_data['add']
                                removed += diff2_data['remove']
                                changed += diff2_data['change']
                        else:
                            if params['deep_debug']:
                                logging.debug(row)
                            logging.info(f"CHANGED {typeId.upper():<20} {i:<20} {column:<20} {row[column,'First']:<10} {'->':^10} {row[column,'Second']:<10}")
                            message = {'typeId': typeId, 'key': settings['key'], 'id': i, 'action': 'change', 'data': {column: row[column,'Second']}, 'data_old': {column: row[column,'First']}}
                            if messages:
                                message_from_messages = [x for x in messages if x['id'] == i]
                                if message_from_messages:
                                    message_from_messages[0]['data'].update({column: row[column,'Second']})
                                    message_from_messages[0]['data_old'].update({column: row[column,'First']})
                                else:
                                    messages.append(message)
                            else:
                                messages.append(message)
            for message in messages:
                logging.debug(f"events.put: {message}")
                #await events.put(message)
                for k, v in message['data_old'].items():
                    if v is None or (isinstance(v, float) and math.isnan(v)):
                        logging.info(f"CHANGED {message['typeId'].upper():<20} {message['id'].upper():<20} {k:<20} {v:<10} {'->':^10} {message['data'][k]:<10} NAN/NONE CHECK PASSED")
                        skipped += 1
                    else:
                        changed += 1
            # if 'save_diff_to_csv' in params:
            #     if params['save_diff_to_csv']:
            #         list_old.sort_values(by=[settings['key']], inplace = True)
            #         list_new.sort_values(by=[settings['key']], inplace = True)
            #         list_old.to_csv(f"data/list_old_{typeId}.csv", index = False, sep = ';', decimal = ',')
            #         list_new.to_csv(f"data/list_new_{typeId}.csv", index = False, sep = ';', decimal = ',')
        if added > 0 or removed > 0 or changed > 0 or skipped > 0:
            return True, {'add': added, 'change': changed, 'skip': skipped, 'remove': removed}
        else:
            return False, {'add': added, 'change': changed, 'skip': skipped, 'remove': removed}



if __name__ == '__main__':
    pass