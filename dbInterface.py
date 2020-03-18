import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import credentials


#get latest list of clients

def que_sync_jobs():
    conn = psycopg2.connect(dbname="client_list", host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'], port=credentials.db_login['port_value'])
    cur = conn.cursor()
    cur.execute(""" 	SELECT * FROM client_details""")

    conn.commit()
    interface_que = cur.fetchall()

    cur.close()
    conn.close()
    return interface_que
	
#check if client exists, application exists, tables exists, create if doesn't exists

def check_client_existence(db_name):
    #, host_value, user_name, password_value, port_value
    try:
        conn = psycopg2.connect(dbname=db_name, host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'], port=credentials.db_login['port_value'])
        conn.close()
        print("DB Exists")
    except:
        conn = psycopg2.connect(dbname="client_list", host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'], port=credentials.db_login['port_value'])
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("""CREATE DATABASE %s WITH 
            OWNER = %s;""" % (db_name, credentials.db_login['user_name']))
        cur.close()
        conn.close()
        print("DB Created")

        conn = psycopg2.connect(dbname=db_name, host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'], port=credentials.db_login['port_value'])
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("""CREATE Table application_List (
            recorded_date_time timestamp,
            application_name char(255),
            application_link_name char(255) )
        """)

        conn.commit()
        cur.execute("""CREATE Table forms_views_List (
                  recorded_date_time timestamp,
                  application_name char(255),
                   application_link_name char(255),
                  form_name char(255),
                  form_link_name char(255),
                  form_link_id int,
                  view_link_id int,
                  view_name char(255),
                   view_link_name char(255))

              """)

        conn.commit()
        cur.close()
        conn.close()
        print("Tables Created")
		
#A function to update applciation list

def update_application_list(application_list, db_name,date_time_stamp):
    list_to_insert = []
    for app in application_list:
        list_to_insert.append([date_time_stamp.strftime('%Y-%m-%d %H:%M:%S'), app[0], app[1]])

    conn = psycopg2.connect(dbname=db_name, host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'], port=credentials.db_login['port_value'])
    cur = conn.cursor()

    cur.executemany("""INSERT INTO application_list (recorded_date_time, application_name, application_link_name) 
    VALUES ( %s, %s, %s);""",list_to_insert)

    conn.commit()
    cur.close()
    conn.close()

# A function to update forms and views for each application

def update_forms_and_views( list_forms_views, application_name, db_name, date_time_stamp):
    list_to_insert = []
    for form_view in list_forms_views:
        list_to_insert.append([date_time_stamp.strftime('%Y-%m-%d %H:%M:%S'), application_name,form_view[0], form_view[1], form_view[2], form_view[3], form_view[4], form_view[5]])
    conn = psycopg2.connect(dbname=db_name, host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'], port=credentials.db_login['port_value'])
    cur = conn.cursor()

    cur.executemany(
        "INSERT INTO forms_views_list (recorded_date_time, application_name, form_name, form_link_name, form_link_id, view_link_id, view_name, view_link_name) VALUES ( %s,%s, %s, %s, %s, %s, %s, %s);",
        list_to_insert)

    conn.commit()
    cur.close()
    conn.close()

# A function to update columns/fields for each view/table

def create_update_view_columns(application_link_name, view_link_name, view_columns, db_name):
    #check if table exists.



    try:
        conn = psycopg2.connect(dbname=db_name, host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'], port=credentials.db_login['port_value'])
        cur = conn.cursor()
        cur.execute("""CREATE table %s_X_%s (
                          record_id char(255),
                          application_name char(255),
                           application_link_name char(255),
                          form_name char(255),
                          form_link_name char(255),
                          view_name char(255),
                           view_link_name char(255)
                           )
                      """% (application_link_name, view_link_name))

        conn.commit()
        cur.close()
        conn.close()
    except:
        print("Table exists")

    cur.close()
    conn.close()
    #get connecting table zoho to SQL tables
    conn2 = psycopg2.connect(dbname="client_list", host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'], port=credentials.db_login['port_value'])
    cur2 = conn2.cursor()

    cur2.execute("""SELECT * FROM zoho_sql_connector""")
    db_connectors = cur2.fetchall()

    cur2.close()
    conn2.close()

    for each_column_new in view_columns:

         for db_connector in db_connectors:

            if str(db_connector[1]) == str(each_column_new[2]):

                try:

                    conn = psycopg2.connect(dbname=db_name, host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'], port=credentials.db_login['port_value'])
                    cur = conn.cursor()

                    cur.execute("""ALTER table %s_X_%s 
                                        ADD %s %s ;
                                          """ % (application_link_name, view_link_name,each_column_new[1],db_connector[2] ))

                    conn.commit()
                    cur.close()
                    conn.close()
                except Exception as e:
                    pass

# A function to get a specific API record, for verification purposes

def check_record(application_link_name, view_link_name, db_name, record_id):
    try:
        conn = psycopg2.connect(dbname=db_name, host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'], port=credentials.db_login['port_value'])
        cur = conn.cursor()

        cur.execute("""SELECT * FROM %s_X_%s where record_id ='%s'""" % (application_link_name, view_link_name, record_id))
        latest_record = cur.fetchone()
        cur.close()
        conn.close()
        return latest_record
    except Exception as e:
        print(e)




# A function to get latest data from ZOHO via API

def update_view_records(application_link_name,
                    view_link_name, records_list, db_name):
    conn = psycopg2.connect(dbname=db_name, host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'], port=credentials.db_login['port_value'])
    cur = conn.cursor()
    print(records_list)
    for record in records_list:

        try:
            cur.execute("""INSERT INTO %s_X_%s (%s) VALUES (%s);"""%
                    (application_link_name, view_link_name, record[0], record[1]))
            conn.commit()
        except Exception as e:
            print(e)
    cur.close()
    conn.close()

# Update listener table, used for important/frequent syncs 

def update_listener_value(application_link_name,
                    view_link_name, record, zc_ownername, auth_token, now):

    print("This is it >>>")
    print(record[2])
    print(record[2][7])
    for each_record in record[2]:
        print(each_record)
    print("<<<<<")

    conn = psycopg2.connect(dbname="client_list", host=credentials.db_login['host_value'],
                            user=credentials.db_login['user_name'], password=credentials.db_login['password_value'],
                            port=credentials.db_login['port_value'])


    cur = conn.cursor()


    try:

        cur.execute("""INSERT INTO listener (application_name, application_link_name, form_name, form_link_name, view_name, view_link_name, auth_token, zc_ownername, start_index, datetime) VALUES ('%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s')
        ON CONFLICT (application_link_name, form_link_name, view_link_name, zc_ownername) DO UPDATE
        SET
        start_index = '%s',
        datetime = '%s';""" %
                        (record[2][1], record[2][2], record[2][3], record[2][4], record[2][5], record[2][6], auth_token, zc_ownername, record[2][7], now, record[2][7], now.strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
    except Exception as e:
        print(e)
    cur.close()
    conn.close()