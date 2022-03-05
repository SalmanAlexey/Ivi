import api
from datetime import datetime
from pyrus.models import requests as re

debug = False


if __name__ == '__main__':
    # print("Starting...")
    # print(api.ivi_client.access_token)
    # print("requesting Registry...")
    # registry = api.ivi_client.get_registry(821582, re.FormRegisterRequest(format='csv'))
    # print(registry)
    # print(registry.error)
    # print(registry.csv)
    # print(len(registry.tasks))
    # exit()






    start_date_text = input("Введите дату начала интервала в формате Число Месяц Год:")
    if not start_date_text:
        start_date = datetime(2022, 2, 1)
    else:
        start_date = datetime.strptime(start_date_text, "%d %m %Y")

    end_date_text = input("Введите дату конца интервала в формате Число Месяц Год:")
    if not end_date_text:
        end_date = datetime(2022, 2, 28)
    else:
        end_date = datetime.strptime(end_date_text, "%d %m %Y")


    error, comments_db = api.create_full_log(start_date, end_date)
    if error:
        print(error)
        exit()

    activity_filename = f"Activity report {start_date.strftime('%y-%m-%d')} {end_date.strftime('%y-%m-%d')}.xlsx"
    worktime_filename = f"Working time report {start_date.strftime('%y-%m-%d')} {end_date.strftime('%y-%m-%d')}.xlsx"
    status_db_filename = f"Status length report DB {start_date.strftime('%y-%m-%d')} {end_date.strftime('%y-%m-%d')}.xlsx"
    status_filename = f"Status report {start_date.strftime('%y-%m-%d')} {end_date.strftime('%y-%m-%d')}.xlsx"

    print("Генерируем отчёт по активности и рабочему времени...")
    activity_report, worktime_report = api.create_activity_worktime_reports(comments_db)
    api.save_activity_report_db(activity_report, activity_filename, xls=True)
    api.save_working_time_report_db(worktime_report, worktime_filename, xls=True)
    del activity_report, worktime_report

    if debug:
        agents = api.get_agents_list(comments_db)
        for agent in sorted(agents):
            print(f"{agent:<25}", end=' ')
            activity_report = api.get_agent_activity(comments_db, agent)
            result, db = api.process_agent_activity(activity_report, agent)
            api.save_agent_debug_db(db, agent + "_db.xlsx", xls=True)
            for item in result:
                print(f"{item:3}", end=" ")
            print("")


    status_db = api.create_status_length_report_db(comments_db, start_date, end_date)
    del comments_db

    status = api.create_status_report(status_db)
    api.save_status_length_report_db(status_db, status_db_filename, xls=True)
    api.save_status_dataframe_to_excel(status, status_filename, "Отчёт по статусам")

