import os
import csv
from datetime import datetime, timedelta, timezone, time, date
from threading import Thread
import xlsxwriter
from time import sleep
import pytz
from io import StringIO
import pyrus.models.requests as re
import pyrus.models.entities as entities
import pyrus
import numpy as np
import pandas
import config
import uuid
from PyrusLogger import PyrusLogger
from config import db_form_id, form_ids, num_of_threads
from pyrus_plus import get_fields_by_id, get_fields_by_name

#################################################################
#   Инициализация клиентов для работы с Pyrus API
#################################################################

# Инициализируем клиент Пайруса от имени пользователя Иви с доступом к задачам
ivi_client = pyrus.client.PyrusAPI(login=config.access_login, security_key=config.access_security_key)
auth_response = ivi_client.auth()

if not auth_response.success:
    print("Неправильный логин или access_token")
    raise SystemExit

# Инициализируем клиент Пайруса от имени бота, чтобы отвечать в задаче-запросе
bot_client = pyrus.client.PyrusAPI(login=config.pyrus_login, security_key=config.security_key)
auth_response = bot_client.auth()

if not auth_response.success:
    print("Неправильный логин или access_token")
    raise SystemExit

# ===============================================================


#################################################################
#   Функции работы с локальной датой
#################################################################

local_tz = pytz.timezone('Europe/Moscow')
base_date = datetime(1900, 1, 1).date()


# Конвертируем объект datetime в локальную временную зону
def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)


# Берем дату в локальной временной зоне
def local_date(date_time):
    return utc_to_local(date_time).date()


# Берем время в локальной временной зоне и приводим его к абстрактному году
def local_time(date_time):
    time_only = utc_to_local(date_time).time()
    return datetime.combine(base_date, time_only)


# Объединяем дату и время комментария в один штамп
def restore_comment_datetime(c_date, c_time):
    return datetime.combine(c_date, c_time.time())

# ===============================================================


#################################################################
#   Обработка комментариев по задаче
#   - считывание в свой массив
#   - форматированная печать на экране
#   - сохранение в файл
#   - эти функции тестируются в модуле test_process_comments.py
#################################################################

def process_task_comments(task):
    # По списку комментариев заполняем массив следующей структурой:
    #  ID задачи
    #  Ответственный
    #  Источник
    #  Дата создания комментария
    #  Время создания комментария
    #  Автор комментария
    #  ID автора
    #  Текст комментария
    #  Действие комментария (задача закрыта, переоткрыта)
    #  Канал
    #  Статус после этого комментария
    #  Первый внешний комментарий
    #  Автор первого внешнего комментария
    #  Дата создания задачи
    #  Статус от прошлого комментария до этого

    # Читаем поля "Ответственный" и "Источник"
    fields = get_fields_by_name(task.flat_fields, responsible='Ответственный', source='Источник')
    comments = list(
        sorted(task.comments, key=lambda comment: comment.create_date))  # Сортируем комментарии по появлению

    simple_comments = []  # Массив куда пишем выборку по комментарию
    status = 'Новая'  # Начальное значение для статуса

    external_comment_number = 1 if fields.source == 'Исх' else 0
    first_external_comment_author = ''
    first_external_comment_date = local_date(task.create_date)
    first_external_comment_time = local_time(task.create_date)
    for idx, comment in enumerate(comments):
        previous_status = status
        if comment.field_updates is not None:  # Обновляем поле статус если менялось данным комментарием
            for field in comment.field_updates:
                if field.name == 'Статус':
                    status = field.value.choice_names[0]  # Заменяем новым статусом
                    break  # Прекращаем обработку если нашли изменение статуса
        if comment.channel is not None:  # Обрабатываем внешний комментарий
            if comment.author.id == 1730:  # Комментарий создан Pyrus - значит, нам кто-то написал
                if status != 'Новая':
                    status = "Ответ поступил"
            else:
                external_comment_number += 1
        if comment.action == 'finished':  # Комментарий, закрывающий задачу должен сразу устанавливать статус Завершена
            status = "Завершена"
        if first_external_comment_author == '' and external_comment_number == 1:
            first_external_comment_author = comment.author.first_name + " " + comment.author.last_name
            first_external_comment_date = local_date(comment.create_date)
            first_external_comment_time = local_time(comment.create_date)
        # Добавляем запись к массиву
        simple_comments.append([task.id,  # ID задачи  ЧИСЛО
                                '' if fields.responsible is None else fields.responsible,  # Ответственный
                                fields.source,  # Источник
                                local_date(comment.create_date),  # Дата комментария
                                local_time(comment.create_date),  # Время комметария
                                comment.author.first_name + " " + comment.author.last_name,  # Автор комментария
                                comment.author.id,  # ID автора ЧИСЛО
                                "+" if comment.text is not None else '',  # Есть текст
                                comment.action if comment.action is not None else '',  # Действие комментария
                                '' if comment.channel is None else comment.channel.type,  # Канал
                                status,  # Статус комментария
                                str(external_comment_number) if
                                (comment.channel is not None and comment.author.id != 1730) or
                                (fields.source == 'Исх' and idx == 0)
                                else '',  # номер внеш. комм.
                                first_external_comment_author,  # Автор первого комм.
                                local_date(task.create_date),  # Дата создания задачи
                                first_external_comment_date,
                                first_external_comment_time,
                                previous_status]  # Прошлый статус
                               )
    return simple_comments


def convert_record_to_text(comment):
    # Конвертируем запись с числами (номер задач) и датами временами (python datetime) в чисто текстовую запись
    # Превращаем массив в массив
    row = [comment[0], comment[1], comment[2], comment[3].strftime("%d/%m/%Y"),
           comment[4].strftime("%H:%M:%S"), comment[5], comment[6], comment[7],
           comment[8], comment[9], comment[10], comment[11], comment[12],
           comment[13].strftime("%d/%m/%Y"), comment[14].strftime("%d/%m/%Y"),
           comment[15].strftime("%H:%M:%S")]
    if len(comment) == 17:  # Если есть новый статус
        row.append(comment[16])
    else:
        row.append("")
    return row


def convert_text_to_record(row):
    # Превращаем текст обратно в запись с датами/временами
    comment = [int(row[0]), row[1], row[2], datetime.strptime(row[3], "%d/%m/%Y").date(),
               datetime.strptime(row[4], "%H:%M:%S"), row[5], int(row[6]), row[7], row[8], row[9], row[10],
               row[11],
               row[12], datetime.strptime(row[13], "%d/%m/%Y").date(),
               datetime.strptime(row[14], "%d/%m/%Y").date(), datetime.strptime(row[15], "%H:%M:%S")]
    if len(row) == 17:  # Если данные новые
        comment.append(row[16])
    else:
        comment.append("")
    return comment


def print_comments_db(comments_db):
    # Печатаем на экране форматируя по столбцам прочитанные комментарии
    if comments_db is not None:
        for comment in comments_db:
            row = convert_record_to_text(comment)
            print(f'{row[0]}, '  # ID задачи
                  f'{row[1]:>20}, '  # Ответственный
                  f'{row[2]:>5} '  # Источник
                  f'{row[3]}, '  # Дата комментария
                  f'{row[4]}, '  # Время комметария
                  f'{row[5]:>20}, '  # Автор комментария
                  f'{row[6]:>7} '  # ID автора
                  f'{row[7]:>2} '  # Есть текст
                  f'{row[8]:>10}, '  # Действие комментария
                  f'{row[9]:>10}, '  # Канал
                  f'{row[10]:>22}, '  # Статус комментария
                  f'{row[11]:>2}, '  # номер внеш. комм.
                  f'{row[12]:>20}, '  # Автор первого комм.
                  f'{row[13]}, '  # Дата создания задачи
                  f'{row[14]}, '  # Дата первого комментария
                  f'{row[15]}', end='')  # Время первого комментария
            if len(row) == 17:  # Поддержка совместимости - если есть новое значение для статуса
                print(f", {row[16]}")
    return


def filter_date(comments_db, date):
    # Фильтрует комментарии только за указанную дату
    return [x for x in comments_db if x[3] == date.date()]


def save_matrix_to_file(matrix, filename, header_row, xls=False, convert_function=None, append=None):
    if xls:
        workbook = xlsxwriter.Workbook(filename)
        worksheet = workbook.add_worksheet()
        bold_centered = workbook.add_format({'bold': True})
        bold_centered.set_align('center')
        date_format = workbook.add_format({'num_format': 'dd/mm/yy'})
        time_format = workbook.add_format({'num_format': 'hh:mm:ss'})
        duration_format = workbook.add_format({'num_format': '[h]:mm:ss'})

        for i, title in enumerate(header_row):
            worksheet.write(0, i, title, bold_centered)

        for line_id, line in enumerate(matrix):
            for row_id, cell in enumerate(line):
                if not isinstance(cell, datetime) and isinstance(cell, date):
                    worksheet.write_datetime(line_id + 1, row_id, cell, date_format)
                elif isinstance(cell, time):
                    worksheet.write_datetime(line_id + 1, row_id, cell, time_format)
                elif isinstance(cell, timedelta):
                    worksheet.write_datetime(line_id + 1, row_id, cell, duration_format)
                elif isinstance(cell, datetime):
                    worksheet.write_datetime(line_id + 1, row_id, cell.time(), time_format)
                elif isinstance(cell, str) and cell and cell.isdigit():
                    worksheet.write(line_id + 1, row_id, int(cell))
                else:
                    worksheet.write(line_id + 1, row_id, cell)

        def get_col_widths(matrix):
            return [max(max(len(str(line[i])) for line in matrix), len(str(header_row[i]))) + 1
                    for i in range(len(header_row))]

        for i, width in enumerate(get_col_widths(matrix)):
            worksheet.set_column(i, i, width)

        workbook.close()

    else:
        if convert_function is None:
            def convert_function(x): return x
        write_mode = "a" if append is not None else "w"
        with open(filename, mode=write_mode, newline='') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if append is None:
                csv_writer.writerow(header_row)
            if matrix is not None:
                for line_id in matrix:
                    row = convert_function(line_id)
                    csv_writer.writerow(row)
                    csv_file.flush()
            del csv_writer
    return


def save_comments_db(comments_db, filename, xls=False, append=None):
    header = ["Task ID", "Ответственный", "Источник", "Дата", "Время", "Автор комментарий", "ID автора",
              "Текст", "Действие", "Канал", "Статус", "Первый?", "Автор первого", "Дата задачи",
              "Дата первого комментария", "Время первого комментария", "Предыдущий статус"]
    save_matrix_to_file(comments_db, filename, header, xls, convert_record_to_text, append)


def old_read_comments_db(filename):
    # Читаем из файла filename обратно в массив comments_db
    with open(filename, mode="r") as csv_file:
        comments_db = []
        csv_reader = csv.reader(csv_file)
        next(csv_reader)
        for idx, row in enumerate(csv_reader):
            print(idx, end=",")
            new_comment = convert_text_to_record(row)
            comments_db.append(new_comment)
            del new_comment

    return comments_db


def read_comments_db(filename):
    # Читаем из файла filename обратно в массив comments_db
    data_file = open(filename, mode="r")
    comments_db = []
    line = data_file.readline()
    if line:
        while True:
            line = data_file.readline()
            if not line:
                break
            line2 = line[:-1].split(',')
            new_comment = convert_text_to_record(line2)
            comments_db.append(new_comment)
            del new_comment

    data_file.close()
    del data_file
    return comments_db

# ===============================================================


#################################################################
#   Функции для работы с формами
#   - чтение параметров из формы с дневным отчетом
#   - запись файла отчета в форму
#   - эти функции тестируются в модуле test_form_tasks.py
#################################################################

def put_db_file(task_id, filename, field_id):
    # Прикладываем файл в соответствующее поле задачи
    guid = bot_client.upload_file(filename).guid
    new_field = entities.FormField(id=field_id, value=[guid])
    bot_client.comment_task(task_id, re.TaskCommentRequest(field_updates=[new_field]))
    return


def read_db_from_task(task, filename, field_id):
    # Считываем из задачи файл и сохраняем его с именем filename
    form_file = get_fields_by_id(task.flat_fields, file=field_id).file
    download_response = ivi_client.download_file(form_file[0].id)
    with open(filename, "wb") as file:
        file.write(download_response.raw_file)
    return


def get_daily_log_create_task_detail(task):
    # Считываем из задачи номер формы и дату, для создания базы данных за день
    data = get_fields_by_id(task.flat_fields, date=2, form_id=1)
    yesterday = None
    if data.date is None:
        yesterday = (datetime.now(local_tz) - timedelta(hours=24))
        yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        data_field = entities.FormField(id=2, value=yesterday.date())
        comment = bot_client.comment_task(task.id, re.TaskCommentRequest(field_updates=[data_field]))
    return data.form_id, data.date if data.date is not None else yesterday


def get_report_create_task_details(task):
    # Считываем из задачи диапазон дат для создания сборного отчёта
    data = get_fields_by_id(task.flat_fields, start=1, end=2)
    return data.start, data.end


def get_db_registry_for_report(start_date, end_date):
    # Возвращает реестр задач, у которых дата по отчёту находится внутри дат
    range_filter = entities.RangeFilter(field_id=2, values=[start_date.date(), end_date.date()])
    reg_request = re.FormRegisterRequest(filters=[range_filter], include_archived=True, field_ids=[1, 2],
                                         format="csv")
    registry = bot_client.get_registry(db_form_id, reg_request)
    csv_file = StringIO(registry.csv)
    reader = csv.reader(csv_file)
    next(reader)
    result = []
    for row in reader:
        result.append(row)
    return result


def check_db_registry(registry, start_date, end_date):
    # Оставляем записи вида <номер формы>, <дата база данных> убирая все служебные записи о реестра
    # Нас интересует, только какие номера форм и даты есть в реестре
    dates_list = [x[-2:] for x in registry]
    current_date = start_date
    missing_list = []
    while True:
        # Формируем строковую запись текущей даты
        current_date_string = current_date.strftime("%Y-%m-%d")

        # Проверяем наличие всех пар (форма-дата) в реестре
        for idx in form_ids:
            if [str(idx), current_date_string] not in dates_list:
                missing_list.append(f"{idx} - {current_date_string}")
        current_date += timedelta(days=1)
        if current_date > end_date:
            break
    if missing_list:
        return f"Ошибка, в базе отсутствуют следующие данные (номер формы - отсутствующая дата): " \
               f"\n{', '.join(missing_list)}"
    else:
        return ""


def get_db_from_daily_log(task_id, db):
    # Получить файл с базой данных из задачи

    task = ivi_client.get_task(task_id).task
    form_file = get_fields_by_id(task.flat_fields, file=5).file
    del task
    if form_file is None:
        return f"No file found in task {task_id}"
    download_response = ivi_client.download_file(form_file[0].id)
    del form_file

    if download_response.error:
        return download_response.error
    else:
        # db_file = StringIO(download_response.raw_file.decode("utf-8"))
        # csv_reader = csv.reader(db_file)
        # next(csv_reader)  # Пропускаем заголовок с названиями
        # for row in csv_reader:
        #     db.append(convert_text_to_record(row))
        # return None, db
        filestring = download_response.raw_file.decode("utf-8").splitlines()
        del download_response
        for line in filestring[1:]:
            db.append(convert_text_to_record(line.split(',')))
        del filestring
        return None

def create_full_log(start_date, end_date):
    # Создаем единую базу данных по всем дням в интервале
    # Файл базы данных, пополняемый по ходу загрузки отдельных дневных баз
    temp_file_name = str(uuid.uuid4())
    save_comments_db([],temp_file_name)

    # Получаем реестр с задачами индивидуальных логов
    registry = get_db_registry_for_report(start_date, end_date)

    # Проверяем реестр задач на наличие всех данных
    error = check_db_registry(registry, start_date, end_date)

    total = len(registry)
    if not error:
        # По каждой записи из реестра загружаем .csv файл и импортируем из него данные в базу
        for idx, task_id in enumerate([x[0] for x in registry]):
            log(f"Processing db daily log from task {task_id} ({idx+1}/{total})")
            daily_db = []
            error = get_db_from_daily_log(int(task_id), daily_db)
            if error:
                del daily_db
                break
            save_comments_db(daily_db, temp_file_name, xls=False, append=True)
            del daily_db

    del total, registry

    print('Читаем полный лог в память')
    full_comments_db = read_comments_db(temp_file_name)
    print('Сортируем лог в памяти...')
    full_comments_db = sorted(full_comments_db, key=lambda x: (x[0], x[3]))  # Сначала сортируем по номерам задачи
    os.remove(temp_file_name)
    return error, full_comments_db

# ===============================================================


#################################################################
#   Загружаем в несколько потоков задачи из списка (полученного выгрузкой реестра)
#   - отдельная функция потока
#   - разделение всего реестра на части
#   - эти функции тестируются в модуле test_process_registry.py
#################################################################


task_processed = 0  # Глобальный счетчкик обработанных задач
total_tasks = 0  # Всего задач на обработку
logger = None  # Обертка для логгера - пишет на экран и в задачу


def get_task_list(form_id, form_register_request):
    # Возвращаем массив номеров задач из реестра в формате сsv
    registry = ivi_client.get_registry(form_id, form_register_request)
    if registry.csv is None:
        return []
    else:
        csv_file = StringIO(registry.csv)
        csv_reader = csv.reader(csv_file)
        next(csv_reader)
        task_ids = []
        for row in csv_reader:
            task_ids.append(int(row[0]))
        return task_ids


def log(text):
    # Печатаем на экране и в задачу
    print(text)
    if logger is not None:
        Thread(target=logger.log, args=[text]).start()
    return


global_comments_db = []


def process_task_list(task_ids):
    # Собрать все комментарии в собственную базу по массиву задач tasks, номер треда - thread_id
    # Увеличиваем с каждой задачей глобальный счетчик обработанных задач
    # Если задать параметр фильтра даты, пишем только комментарии по одной этой дате
    global task_processed, global_comments_db

    for idx in task_ids:
        failed = False
        for i in range(10):
            try:
                task = ivi_client.get_task(idx).task
                global_comments_db.extend(process_task_comments(task))
                break
            except Exception as e:
                print(f"Ошибка при загрузке задачи {idx}, ждем {2 ** i}c и делаем попытку {i + 2} (всего 10 попыток).",
                      f"Exception: {e}")
                sleep(2 ** i)
                if i == 9:
                    failed = True
        if failed:
            log(f"Task {idx} failed")

        task_processed += 1
        if task_processed % 100 == 0:
            log(f"Обработано [{task_processed}/{total_tasks}] задач")

    return


def split_array(source, shift, num_of_threads):
    # Функция разделения массива на части (для передачи в отдельные потоки)
    length = len(source)
    chunk = length // num_of_threads + 1

    return source[chunk * shift:chunk * (shift + 1)]


def remove_csv():
    # Удаляем остатки старых файлов с отчетами с локального диска
    current_dir = os.listdir()
    for item in current_dir:
        if item.endswith(".csv"):
            os.remove(item)


def create_comments_db_and_daily_reports_for_tasks_list(tasks_ids, date):
    # Обрабатывает весь реестр в несколько потоков
    # Фильтруя по дате
    # Сохраняет в файлы
    #   - лог комментариев в comments_db.csv
    #   - отчет об активности за день в activity_report.csv
    #   - отчет о рабочем времени в worktime_report.csv
    #   - БД для рассчета статусов в status_report_db.csv

    global total_tasks, task_processed, global_comments_db

    global_comments_db = []
    total_tasks = len(tasks_ids)
    task_processed = 0
    log(f"Total: {total_tasks}")

    threads_list = []

    for i in range(num_of_threads):
        new_thread = Thread(target=process_task_list, args=[split_array(tasks_ids, i, num_of_threads)])
        threads_list.append(new_thread)
        new_thread.start()

    for t in threads_list:
        t.join()

    # Очищаем поляну
    remove_csv()

    comments_db = sorted(filter_date(global_comments_db, date), key=lambda x: x[0])
    save_comments_db(comments_db, "comments_db.csv")
    activity, working_time = create_activity_worktime_reports(comments_db)
    save_activity_report_db(activity, "activity_report.csv")
    save_working_time_report_db(working_time, "worktime_report.csv")
    status = create_status_length_report_db(comments_db, date, date)
    save_status_length_report_db(status, 'status_report_db.csv')

    del threads_list
    del global_comments_db
    return


def get_register_by_date_msk(form_id, date):
    # Возвращает задачи формы с номером form_id, в которых _возможно_ были комментарии в указанную дату
    # Дата создания берётся по временной зоны Москвы
    # Для этого запрашиваем реестр формы с задачами созданными до date + 1 день и в которых были новые комментарии после
    # даты date

    start_date = date - timedelta(hours=3)  # Сдвигаем начало на три часа
    next_day = date + timedelta(days=1)
    task_ids = get_task_list(form_id, form_register_request=re.FormRegisterRequest(include_archived=True,
                                                                                   modified_after=start_date,
                                                                                   created_before=next_day,
                                                                                   format="csv",
                                                                                   simple_format=False,
                                                                                   field_ids=[1]))
    return task_ids

# ===============================================================


#################################################################
#   Загружаем в несколько потоков задачи из списка (полученного выгрузкой реестра)
#   - отдельная функция потока
#   - разделение всего реестра на части
#   - эти функции тестируются в модуле test_process_registry.py
#################################################################

def create_intermediate_db_for_date(task):
    global logger

    """
    Основная функция создания промежуточной БД - по номеру задачи считываем данные для создания промежуточной БД, 
    загружаем реестр за указанную дату и проходим по всем задачам реестра, собирая комментарии в базу и
    периодически записывая статус в задачу
    """
    logger = PyrusLogger(bot_client, task.id)

    form_id, date = get_daily_log_create_task_detail(task)

    log("Запускаем расчет статистики")
    print("Запрашиваем реестр...")
    registry = get_register_by_date_msk(form_id, date)
    print("Реестр получен!")
    log("Запуск создания базы данных комментариев. Процесс может занять несколько минут")

    if registry is None:
        bot_client.comment_task(task.id, task_comment_request=re.TaskCommentRequest(
            text="Нет доступных задач за данный период",
            approval_choice="rejected"))
        return

    create_comments_db_and_daily_reports_for_tasks_list(registry, date)

    suffix = f"{form_id}_{date.strftime('%Y%m%d')}.csv"
    comment_db_filename = f"comments_db_{suffix}"
    activity_report_db_filename = f"activity report_{suffix}"
    worktime_report_db_filename = f"worktime report_{suffix}"
    status_report_db_filename = f"status report DB_{suffix}"
    os.rename("comments_db.csv", comment_db_filename)
    os.rename("activity_report.csv", activity_report_db_filename)
    os.rename("worktime_report.csv", worktime_report_db_filename)
    os.rename("status_report_db.csv", status_report_db_filename)
    put_db_file(task.id, comment_db_filename, 5)
    put_db_file(task.id, activity_report_db_filename, 6)
    put_db_file(task.id, worktime_report_db_filename, 8)
    put_db_file(task.id, status_report_db_filename, 9)
    bot_client.comment_task(task.id, task_comment_request=re.TaskCommentRequest(
        text=f"Статистика за день готова",
        approval_choice="approved"))

    remove_csv()

    return

# ===============================================================


#################################################################
#   Функции подсчета различных элементов статистики
#   - тестируем в модуле test_analytics
#################################################################


def get_agents_list(comments_db):
    # Получает список агентов, по которым есть вывод отчета
    technical_records = [' Pyrus.com', 'Оператор поддержки ', 'Завершение задачи']
    agents = [x[1] for x in comments_db]  # Ответственные
    agents.extend([x[5] for x in comments_db if x[5] not in technical_records])  # Авторы комментариев
    return list(set(agents))


def get_agents_department():
    contacts = ivi_client.get_contacts()
    agents_dep = {}
    if contacts is not None and contacts.organizations:
        for org in contacts.organizations:
            if org.name == 'ivi':
                for person in org.persons:
                    agents_dep[person.first_name + ' ' + person.last_name] = person.department_name
    return agents_dep


def get_agent_activity(comments_db, agent_name):
    # Получаем список действий затрагивающих агента
    # Либо он - ответственный (это нужно для задач звонков)
    # Либо он - автор комментария

    # Номера задач, относящихся к агенту
    participated = set([x[0] for x in comments_db if x[1] == agent_name or x[5] == agent_name])

    # Возвращаем все комментарии по относящимся к нему задачам, в т.ч. от Pyrus
    return [x for x in comments_db if x[0] in participated]


def last_comment(comments_db):
    # Оставляем последний комментарий от каждой задачи
    last_comments_db = []
    size = len(comments_db)
    for idx, comment in enumerate(comments_db):
        if idx == size - 1:
            last_comments_db.append(comment)
        elif comment[0] != comments_db[idx + 1][0]:
            last_comments_db.append(comment)
    return last_comments_db


def process_agent_activity(comments_db, agent_name):
    # Проходим по списку задач и считаем число обращений и другие параметры

    # Тип обращения - Зв. и комментарий от Оператора поддержка
    # Фильтруются все задачи, относящиеся ко звонкам
    # Последнее условие фильтрует звонки, созданные вчера, но к которым запись приложилась сегодня
    call_comments = [x for x in comments_db if x[2] == 'Зв.' and x[6] == 420726 and x[13] == x[3]]

    # Исключаем двойные записи по одной задаче, так как иногда прикладывается оценка - берем только множество задач
    call_comments_tasks = set([x[0] for x in call_comments])
    calls = len(call_comments_tasks)

    # Фильтруем емейл комментарии
    # Комментарий с типом Исх, Зв. или Mail
    # Есть номер комментария (ответ во внешний канал с непустым текстом)
    # Автор комментария - это агент
    # Исключаем комментарии, закрывающие задачу без текста
    all_email_comments = [x for x in comments_db if x[2] in ['Исх', 'Зв.', 'Mail'] and
                          x[5] == agent_name and
                          x[11] != '' and
                          not (x[8] == 'finished' and x[7] == '')]

    # За емейлы считаем всё, где были емейл комментарии кроме звонков
    all_email_comments_tasks = set([x[0] for x in all_email_comments]) - call_comments_tasks
    emails = len(all_email_comments_tasks)

    # Комментарии не с типом звонок или email
    # Есть номер комментария (ответ во внешний канал с непустым текстом)
    # Автор комментария - это агент
    # Исключаем комментарии, закрывающие задачу без текста
    all_other_comments = [x for x in comments_db if x[2] not in ['Исх', 'Зв.', 'Mail'] and
                          x[5] == agent_name and
                          x[11] != '' and
                          not (x[8] == 'finished' and x[7] == '')]

    all_other_comments_tasks = set([x[0] for x in all_other_comments])
    others = len(all_other_comments_tasks)

    # Комментарии только с емейлами, исключаем звонки
    all_email_only_comments = [x for x in all_email_comments if x[2] != 'Зв.']

    # Комментарии емейлы где ответственный - я
    own_email_comments = [x for x in all_email_comments if x[12] == agent_name]
    # Комментарии емейлы с датой комментария отличной от даты первого комментария
    own_in_progress_email_comments = [x for x in own_email_comments if x[3] != x[14]]
    own_in_progress_email_tasks = set([x[0] for x in own_in_progress_email_comments])

    # Комментарии емейлы где ответственный - не я
    others_email_comments = [x for x in all_email_comments if x[12] != agent_name]
    others_email_comments_tasks = set([x[0] for x in others_email_comments])

    # Комментарии другие где ответственный - я
    own_other_comments = [x for x in all_other_comments if x[12] == agent_name]
    # Комментарии другие с датой комментария отличной от даты первого комментария
    own_in_progress_other_comments = [x for x in own_other_comments if x[3] != x[14]]
    own_in_progress_other_tasks = set([x[0] for x in own_in_progress_other_comments])

    # Комментарии другие где ответственный - не я
    others_other_comments = [x for x in all_other_comments if x[12] != agent_name]
    others_other_comments_tasks = set([x[0] for x in others_other_comments])

    # Первый комментарий email - номер 1 и не в Звонке
    first_email_comments = [x for x in all_email_comments if x[11] == '1' and x[2] != 'Зв.']
    first_email_comments_tasks = set(x[0] for x in first_email_comments)
    first_other_comments = [x for x in all_other_comments if x[11] == '1']
    first_other_comments_tasks = set(x[0] for x in first_other_comments)

    # Новые задачи - это звонки + задачи с первым комментарием в канал
    new_tasks = first_email_comments_tasks.union(call_comments_tasks).union(first_other_comments_tasks)
    new = len(new_tasks)

    # Свои в работе
    own_in_progress_tasks = own_in_progress_email_tasks.union(own_in_progress_other_tasks) - new_tasks
    own_in_progress = len(own_in_progress_tasks)

    # Чужие в работе
    others_in_progress_tasks = others_email_comments_tasks.union(others_other_comments_tasks)
    others_in_progress = len(others_in_progress_tasks)

    # Новая версия - комментарии по звонкам, емейлам и прочим
    active_tasks = sorted([x for x in comments_db if x[0] in
                           call_comments_tasks.union(all_email_comments_tasks).union(all_other_comments_tasks)],
                          key=lambda x: (x[0], x[3]))
    statuses = last_comment(active_tasks)

    wait_user_status_tasks = [x[0] for x in statuses if x[10] in ['Ожидает ответа клиента',
                                                                  'Повторный запрос информации']]
    wait_user_status = len(wait_user_status_tasks)

    wait_2nd_line_status_tasks = [x[0] for x in statuses if x[10] == 'Ожидает ответа 2-й линии']
    wait_2nd_line_status = len(wait_2nd_line_status_tasks)

    closed_status_tasks = [x[0] for x in statuses if x[10] in ['Завершена', 'Завершена автоматически',
                                                               'Не требует ответа']]
    closed_status = len(closed_status_tasks)

    response_status_tasks = [x[0] for x in statuses if x[10] == 'Ответ поступил']
    response_status = len(response_status_tasks)

    new_status_tasks = [x[0] for x in statuses if x[10] == 'Новая']
    new_status = len(new_status_tasks)

    # Генерируем выборку комментариев по агенту с пометками, куда посчиталась
    debug_comments_db = []
    for comment in comments_db:
        call, email, other, new_task, own_in_work, others_in_work, wait, wait2nd, closed, response, new_st = [''] * 11
        # if comment in last_comment_db: # Дописываем к последнему комментарию к задаче статус по выборкам
        task_id = comment[0]
        if task_id in call_comments_tasks:
            call = '+'
        elif task_id in all_email_comments_tasks:
            email = '+'
        elif task_id in all_other_comments_tasks:
            other = '+'
        if task_id in new_tasks:
            new_task = '+'
        elif task_id in own_in_progress_tasks:
            own_in_work = '+'
        elif task_id in others_in_progress_tasks:
            others_in_work = '+'
        if task_id in wait_user_status_tasks:
            wait = '+'
        elif task_id in wait_2nd_line_status_tasks:
            wait2nd = '+'
        elif task_id in closed_status_tasks:
            closed = '+'
        elif task_id in response_status_tasks:
            response = '+'
        elif task_id in response_status_tasks:
            new_st = '+'
        debug_comments_db.append(comment + [call, email, other, new_task, own_in_work, others_in_work, wait, wait2nd,
                                            closed, response, new_st])

    return [calls + emails + others, calls, emails, others, new, own_in_progress, others_in_progress,
            wait_user_status, wait_2nd_line_status, closed_status, response_status, new_status], \
           sorted(debug_comments_db, key=lambda x: x[0])


def get_agent_working_time(comments_db, agent):
    # Заполняем базу данных данными о времени первого и последнего комментария от данного агента

    # Отбираем комментарии, созданные агентом
    own_comments = [x for x in comments_db if x[5] == agent]
    # Отбираем даты комментариев
    dates = set([x[3] for x in own_comments])

    working_hours_db = []
    for date in dates:
        date_text = date.strftime('%d/%m/%Y')

        date_comments_times = sorted([x[4] for x in own_comments if x[3] == date])  # Отсортированное время комментариев

        first_record = date_comments_times[0]  # Первая запись
        last_record = date_comments_times[-1]  # Последняя запись

        max_shift_length = timedelta(hours=12, minutes=30)
        if last_record - first_record > max_shift_length:  # Если разница во времени больше чем смена
            split_time = first_record + max_shift_length
        else:
            split_time = last_record
        first_half_times = [x for x in date_comments_times if x <= split_time]
        second_half_times = [x for x in date_comments_times if x > split_time]  # Если смена за день одна, то это пусто

        for array in [first_half_times, second_half_times]:
            if array:
                working_hours_db.append([agent, date_text, array[0].strftime('%H:%M:%S'),
                                         array[-1].strftime('%H:%M:%S')])

    return sorted(working_hours_db, key=lambda x: x[1])


def create_activity_worktime_reports(comments_db):
    activity_report_db = []
    working_time_report_db = []
    agents = get_agents_list(comments_db)
    for agent in sorted(agents):
        activity = get_agent_activity(comments_db, agent)
        result, agent_db = process_agent_activity(activity, agent)
        if set(result) != {0}:
            new_line = [agent]
            new_line.extend(result)
            activity_report_db.append(new_line)
        working_time_report_db.extend(get_agent_working_time(activity, agent))

    return activity_report_db, working_time_report_db


def create_status_length_report_db(comments_db, start_date, end_date):
    # База данных и границы диапазона
    if not comments_db:
        return []

    agent_dep = get_agents_department()

    range_start = datetime.combine(start_date, time())
    range_end = datetime.combine(end_date, time(23, 59, 59))

    new_task, agent_is_working, waiting_2nd_line, waiting_client, waiting_rerequest = [timedelta()] * 5

    def update_status_time(status, status_increase):
        nonlocal new_task, agent_is_working, waiting_2nd_line, waiting_client, waiting_rerequest
        if status in ['Новая']:
            new_task += status_increase
        elif status in ['Ответ поступил']:
            agent_is_working += status_increase
        elif status in ['Ожидает ответа клиента']:
            waiting_client += status_increase
        elif status in ['Повторный запрос информации']:
            waiting_rerequest += status_increase
        elif status in ['Завершена', 'Не требует ответа', 'Завершена автоматически']:
            pass
        else:
            waiting_2nd_line += status_increase

    current_task_id = comments_db[0][0]  # Set task ID to first comment task id
    current_responsible = comments_db[0][1]
    current_source = comments_db[0][2]
    last_comment_in_current_task = None  # Pointer to previous comment and previous timestamp
    last_comment_in_current_task_datetime = None

    status_report_db = []

    for comment in comments_db:
        if comment[0] != current_task_id:  # Начинаем обрабатывать новую задачу
            # Сохраняем запись по предыдущей задаче

            # Если предыдущая задача ещё не завершена - добавляем к последнему статусу время до конца диапазона
            status = last_comment_in_current_task[10]
            if status != 'Завершена':
                status_increase = range_end - last_comment_in_current_task_datetime
                update_status_time(status, status_increase)
            # Обновляем базу
            status_report_db.append([current_task_id, current_responsible, agent_dep.get(current_responsible, ""),
                                     current_source, waiting_client,
                                     waiting_rerequest, waiting_2nd_line, agent_is_working, new_task])

            # Обнуляем счётчики и указатель на предыдущий комментарий
            new_task, agent_is_working, waiting_2nd_line, waiting_client, waiting_rerequest = [timedelta()] * 5
            last_comment_in_current_task = None
            last_comment_in_current_task_datetime = None

            # Ставим указатель на текущую задачу
            current_task_id = comment[0]
            current_responsible = comment[1]
            current_source = comment[2]

        # Продолжаем обрабатывать текущую задачу
        status = comment[16]
        current_comment_datetime = restore_comment_datetime(comment[3], comment[4])
        if last_comment_in_current_task is None:  # Если это первый комментарий по задаче в диапазоне
            if status == 'Новая':  # Новая задача, первый комментарий
                # Таймеры увеличивать не надо, просто передвигаемся к другому комментарию
                pass
            if status != 'Новая':
                # Если задача не создана этим комментарием, то добавляем к соответствующему статусу время, проведенное
                # от начала периода до момента комментария
                status_increase = current_comment_datetime - range_start
                update_status_time(status, status_increase)
        else:  # Итак, есть предыдущий комментарий в диапазоне
            status_increase = current_comment_datetime - last_comment_in_current_task_datetime
            update_status_time(status, status_increase)

        # Ставим указатель предыдущего комментария
        last_comment_in_current_task = comment
        last_comment_in_current_task_datetime = current_comment_datetime

    return status_report_db


def create_status_report(status_report_db):
    header_row = ["Task ID", "Ответственный", "Департамент", "Источник", "Ожидаем ответа клиента",
                  "Повторный запрос информации", "Ожидаем ответа 2-й линии", "Ответ поступил", "Новая"]
    db = pandas.DataFrame(status_report_db, columns=header_row)
    # Конвертируем timedelta64 в секунды в столбце времени 2-й линии и заменяем 0 на NaN, чтобы по ним не считалось среднее
    db['Ожидаем ответа 2-й линии'] = db["Ожидаем ответа 2-й линии"] / np.timedelta64(1, 's')
    db['Ожидаем ответа 2-й линии'] = db["Ожидаем ответа 2-й линии"].replace(0, np.NaN)

    report_cols = ["Группировка"] + header_row[4:]
    report = pandas.DataFrame(columns=report_cols)

    # Считаем общее среднее, убираем номера задач
    global_mean = db.iloc[:, 1:].mean()
    # Добавляем в отчет
    report = report.append(global_mean, ignore_index=True)
    report.iloc[0, 0] = 'Все'

    # Считаем среднее по источникам
    source_mean = db.iloc[:, 3:].groupby('Источник').mean(numeric_only=False).reset_index()
    source_mean.columns = report_cols

    # Считаем среднее по департаментам
    department_mean = db.iloc[:, 2:].groupby(['Департамент','Источник']).mean(numeric_only=False).reset_index()
    department_mean['Департамент'] = department_mean['Департамент'].replace("", 'Без департамента')
    department_mean['Группировка'] = department_mean['Департамент'] + ' - ' + department_mean['Источник']
    department_mean.drop(['Департамент', 'Источник'], axis='columns', inplace=True)
    department_mean = department_mean[report_cols]

    # Считаем среднее по ответственным
    responsible_mean = db.iloc[:, [1, 4, 5, 6, 7, 8]].groupby('Ответственный').mean(numeric_only=False).reset_index()
    responsible_mean.columns = report_cols
    responsible_mean.iloc[0, 0] = 'Без ответственного'

    # Объединяем все отчеты
    report = pandas.concat([report, source_mean, department_mean, responsible_mean], ignore_index=True)
    # Превращаем NaN в нули и конвертируем в timedelta64 в столбце 2-й линии
    report = report.replace(np.NaN, 0)
    report['Ожидаем ответа 2-й линии'] = report["Ожидаем ответа 2-й линии"] * np.timedelta64(1, 's')

    report['Чистое время решения'] = report['Новая'] + report['Ответ поступил'] + report['Ожидаем ответа 2-й линии']
    report['SL поддержки'] = report['Новая'] + report['Ответ поступил']
    report['Общее время решения'] = report.iloc[:, 1:6].sum(axis=1)

    for idx in range(1, 9):
        report.iloc[:, idx] = report.iloc[:, idx] + pandas.Timestamp('1900-01-01')

    report.set_index('Группировка')

    return report


def save_agent_debug_db(agent_debug_db, filename, xls=False):
    header_row = ["Task ID", "Ответственный", "Источник", "Дата", "Время", "Автор комментарий", "ID автора",
                  "Текст", "Действие", "Канал", "Статус", "Первый?", "Автор первого", "Дата задачи",
                  "Дата первого комментария", "Время первого комментария", "Предыдущий статус", "Всего (телефон)",
                  "Всего (email)", "Всего (прочии)", "Новые",
                  "В работе (свои)", "В работе (других)", "Ожидаем отв. польз.", "Ожидание 2-й линии",
                  "Перешло в Завершил", "В работе - Отв. пост.", "Новые - не взяты"]
    save_matrix_to_file(agent_debug_db, filename, header_row, xls)


def save_activity_report_db(activity_report_db, filename, xls=False):
    header_row = ["ФИО", "Всего за период", "Всего (телефон)", "Всего (email)", "Всего (прочии)", "Новые",
                  "В работе (свои)", "В работе (других)", "Ожидаем отв. польз.", "Ожидание 2-й линии",
                  "Перешло в Завершил", "В работе - Отв. пост.", "Новые - не взяты"]
    save_matrix_to_file(activity_report_db, filename, header_row, xls)


def save_working_time_report_db(working_time_report_db, filename, xls=False):
    header_row = ["ФИО", "Дата", "Начало работы", "Конец работы"]
    save_matrix_to_file(working_time_report_db, filename, header_row, xls)


def save_status_length_report_db(status_length_report_db, filename, xls=False):
    header_row = ["Task ID", "Ответственный", "Департамент", "Источник", "Ожидаем ответа клиента",
                  "Повторный запрос информации", "Ожидаем ответа 2-й линии", "Ответ поступил", "Новая"]
    save_matrix_to_file(status_length_report_db, filename, header_row, xls)


def save_status_dataframe_to_excel(df, filename, sheetname):
    writer = pandas.ExcelWriter(filename, engine='xlsxwriter', datetime_format='[h]:mm:ss')
    df.to_excel(writer, sheet_name=sheetname)  # send df to writer
    worksheet = writer.sheets[sheetname]  # pull worksheet object
    for idx, col in enumerate(df):  # loop through all columns
        series = df[col]
        largest_item = series.astype(str).map(len).max()  # len of largest item
        header_len = len(str(series.name))  # len of column name/header
        max_len = max(largest_item, header_len) + 1  # adding a little extra space
        worksheet.set_column(idx + 1, idx + 1, max_len)  # set column width
    writer.save()


# ===============================================================

#################################################################
#   Функции генерации статистике по собранной базе данных
#   - тестируем в модуле test_get_data4report.py
#################################################################


def generate_report(task):
    global logger
    start_date, end_date = get_report_create_task_details(task)
    print(f"Получены данные: {start_date}, {end_date}")

    # Проверяем фукнцию получения реестра и проверки полноты данных
    # registry = api.get_db_registry_for_report(start_date, end_date)
    # print(api.check_db_registry(registry, start_date, end_date))
    logger = PyrusLogger(bot_client, task.id)

    # Проверяем создание общего лога данных по задачам за период
    error, db = create_full_log(start_date, end_date)
    if error:
        log(error)
        bot_client.comment_task(task.id, re.TaskCommentRequest(text=f"{error}", approval_choice='rejected'))
    else:
        # Генерируем имена файлов
        activity_filename = f"Activity report {start_date.strftime('%y-%m-%d')} {end_date.strftime('%y-%m-%d')}.xlsx"
        worktime_filename = f"Working time report {start_date.strftime('%y-%m-%d')} {end_date.strftime('%y-%m-%d')}.xlsx"
        status_db_filename = f"Status length report DB {start_date.strftime('%y-%m-%d')} {end_date.strftime('%y-%m-%d')}.xlsx"
        status_filename = f"Status report {start_date.strftime('%y-%m-%d')} {end_date.strftime('%y-%m-%d')}.xlsx"

        log("Генерируем отчёт по активности и рабочему времени...")
        activity_db, working_time_db = create_activity_worktime_reports(db)
        save_activity_report_db(activity_db, activity_filename, xls=True)
        save_working_time_report_db(working_time_db, worktime_filename, xls=True)
        del activity_db, working_time_db
        file_guids = [bot_client.upload_file(activity_filename).guid,
                      bot_client.upload_file(worktime_filename).guid]
        bot_client.comment_task(task.id, re.TaskCommentRequest(text="Прикладываем отчёты активности и раб. времени", approval_choice='approved', attachments=file_guids))

        os.remove(activity_filename)
        os.remove(worktime_filename)

        log("Генерируем отчёт по статусам...")
        status_db = create_status_length_report_db(db, start_date, end_date)
        del db
        status = create_status_report(status_db)
        save_status_length_report_db(status_db, status_db_filename, xls=True)
        save_status_dataframe_to_excel(status, status_filename, "Отчёт по статусам")
        file_guids = [bot_client.upload_file(status_db_filename).guid,
                      bot_client.upload_file(status_filename).guid]
        bot_client.comment_task(task.id, re.TaskCommentRequest(text="Прикладываем отчёт по статусам", approval_choice='approved',attachments=file_guids))
        del status_db, status
        os.remove(status_db_filename)
        os.remove(status_filename)

    return
