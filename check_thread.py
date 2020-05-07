#!/usr/local/bin/python3.7
import pymysql
import time
import json
import requests
import schedule
import argparse
from settings import DBLIST_INFO


def connectdb(args, **dblist):
    # 获取执行时间大于指定时间args.t的SQL
    sql = '''
        select
            id,
            user,
            host,
            db,
            time,
            info,
            sha(
                concat(
                    id,
                    ifnull(host, ''),
                    ifnull(db, ''),
                    info
                )
            ) as vsha
        from
            information_schema.`processlist`
        where
            info <> ''
        and time > %s
        and user <>'mysqldump'
     ''' % (args.t)

    try:
        conn = pymysql.connect(host=dblist['host'], user=dblist['username'], passwd=dblist['password'],
                               port=dblist['port'],
                               cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()
        cursor.execute(sql)
        tmp_result = cursor.fetchall()
        if not tmp_result:
            tmp_result = list(tmp_result)
        return tmp_result

    except Exception as e:
        print('Error By connectdb %s' % e)

    finally:
        cursor.close()
        conn.close()


def checkthred(plist, result):
    try:
        # 判断session是否新SQL，若是，则钉钉推送，若不是，则更新time的值
        if not plist:
            for v_sql in result:
                plist.append(v_sql)
                ding_md = "# 发现SLOW SQL \n- **线程ID**: %s\n - **用  户:**  %s\n - **客户端:** %s\n- **数据库:** %s\n- **执行时间:** %s秒 \n- **SQL:** \n> %s" % (
                    v_sql['id'], v_sql['user'], v_sql['host'], v_sql['db'], v_sql['time'], v_sql['info'])
                dingding_robot(ding_md)
        else:
            for active_session in result:
                for i in range(len(plist)):
                    if active_session['vsha'] != plist[i]['vsha']:
                        if i == len(plist) - 1:
                            plist.append(active_session)
                            ding_md = "# 发现SLOW SQL \n- **线程ID**: %s\n - **用  户:**  %s\n - **客户端:** %s\n- **数据库:** %s\n- **执行时间:** %s秒  \n- **SQL:** \n> %s" % (
                                active_session['id'], active_session['user'], active_session['host'],
                                active_session['db'],
                                active_session['time'], active_session['info'])
                            dingding_robot(ding_md)
                        else:
                            continue
                    else:
                        plist[i]['time'] = active_session['time']
                        break

        # 判断session是否执行完毕，若是，则从列表中删除此sql，并钉钉推送执行完成，若否，则不做处理
        if not result:
            for index, plist_session in enumerate(plist):
                ding_md = "# SLOW SQL执行完成 \n- **线程ID:**  %s \n- **用  户:**  %s \n- **客户端:** %s \n- **数据库:** %s \n - **执行时间:** %s秒\n- **SQL:** \n> %s" % (
                    plist_session['id'], plist_session['user'], plist_session['host'], plist_session['db'],
                    plist_session['time'], plist_session['info'])
                dingding_robot(ding_md)
                plist.pop(index)

        elif plist:
            for index, plist_session in enumerate(plist):
                for i in range(len(result)):
                    if plist_session['vsha'] == result[i]['vsha']:
                        break
                    else:
                        if plist_session['vsha'] != result[i]['vsha'] and i == len(result) - 1:
                            ding_md = "# SLOW SQL执行完成 \n- **线程ID:**  %s \n- **用  户:**  %s \n- **客户端:** %s \n- **数据库:** %s \n - **执行时间:** %s秒\n- **SQL:** \n> %s" % (
                                plist_session['id'], plist_session['user'], plist_session['host'], plist_session['db'],
                                plist_session['time'], plist_session['info'])
                            dingding_robot(ding_md)
                            plist.pop(index)
                        else:
                            continue
        else:
            pass

    except Exception as e:
        print("Error by checkthred %s" % e)
        exit(-1)


def dingding_robot(content):
    webhook = "https://oapi.dingtalk.com/robot/send?access_token=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    headers = {'content-type': 'application/json'}
    data = {"msgtype": "markdown", "markdown": {"title": "SLOW SQL监控", "text": content},
            "at": {"atMobiles": ["15731693317"]}}
    r = requests.post(webhook, headers=headers, data=json.dumps(data))
    r.encoding = 'utf-8'
    return (r.text)


def main(args):
    global result
    result = []
    for db in DBLIST_INFO:
        result = result + connectdb(args, **db)

    checkthred(plist, result)


if __name__ == "__main__":
    global plist
    plist = []

    # 指定session的执行时间，默认5秒
    parser = argparse.ArgumentParser(usage='%(prog)s [options]')
    parser.add_argument('-t', default='5', metavar='--time', help='time active threads run')
    args = parser.parse_args()

    # main()

    schedule.every(2).seconds.do(main, args)  ## 定时任务，每两秒执行一次
    while True:
        schedule.run_pending()
        time.sleep(1)
