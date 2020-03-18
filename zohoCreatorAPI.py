import requests
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import datetime
import xml.etree.ElementTree as xmlTree

# Get latest list of clients

def que_sync_jobs(db_name, host_value, user_name, password_value, port_value):

    conn = psycopg2.connect(dbname=db_name, host=host_value,
                            user=user_name, password=password_value, port=port_value)
    cur = conn.cursor()
    cur.execute(""" 	SELECT * FROM client_details""")

    conn.commit()
    interface_que = cur.fetchall()

    cur.close()
    conn.close()
    return interface_que


# Check if client exists, create a DB and tables if doesn't

def check_client_existence(db_name, host_value, user_name, password_value, port_value):

    try:
        conn = psycopg2.connect(dbname=db_name, host=host_value,
                            user=user_name, password=password_value, port=port_value)
        conn.close()
        print("DB Exists")
    except:
        conn = psycopg2.connect(dbname="client_list", host=host_value,
                                user=user_name, password=password_value, port=port_value)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("""CREATE DATABASE %s WITH 
            OWNER = %s;""" % (db_name, user_name))
        cur.close()
        conn.close()
        print("DB Created")

        conn = psycopg2.connect(dbname=db_name, host=host_value,
                                user=user_name, password=password_value, port=port_value)
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

#Get client's list of applications

def get_application_list(api_endpoint, api_data):
    application_list = []
    xml_tree_list = []
    # sending post request and saving response as response object
    r = requests.get(url=api_endpoint, params=api_data)

    # extracting response text
    api_output = r.text
    xml_tree_list = xmlTree.fromstring(api_output)

    for app_list in xml_tree_list.findall("./result/application_list/applications/application"):
        application_list.append([(app_list.find('application_name').text),(app_list.find('link_name').text).replace("-","_")])
    return application_list

#Update client's list of applications, for syncing and listeners' purposes
def update_application_list(application_list, db_name, host_value, user_name, password_value, port_value, date_time_stamp):
    list_to_insert = []
    for app in application_list:
        list_to_insert.append([date_time_stamp.strftime('%Y-%m-%d %H:%M:%S'), app[0], app[1]])

    conn = psycopg2.connect(dbname=db_name, host=host_value,
                            user=user_name, password=password_value, port=port_value)
    cur = conn.cursor()

    cur.executemany("""INSERT INTO application_list (recorded_date_time, application_name, application_link_name) 
    VALUES ( %s, %s, %s);""",list_to_insert)

    conn.commit()
    cur.close()
    conn.close()

# Get forms for specified application

def get_forms_and_views(api_endpoint, api_data):
    list_forms = []
    list_forms_views = []
    # sending post request and saving response as response object
    r = requests.get(url=api_endpoint, params=api_data)

    api_output = r.text
    xml_tree_list = xmlTree.fromstring(api_output)
    for form_view in xml_tree_list.findall("./application/"):

        for each_form in form_view.findall("./form"):
            list_forms.append([each_form.find('displayname').text, each_form.find('componentname').text,(each_form.find('linkid').text).replace("-", "_")])
        for each_view in form_view.findall("./view"):
            for each_form in list_forms:
                if each_form[1] == each_view.find('formlinkname').text:
                    list_forms_views.append([each_form[0],each_form[1],each_form[2],(each_view.find('linkid').text).replace("-","_"),(each_view.find('componentname').text),(each_view.find('displayname').text)])
        #list_forms_views.append([(form_view.find('application_name').text), (form_view.find('link_name').text)])
    return list_forms_views

#Update forms and views on DBs

def update_forms_and_views( list_forms_views, application_name, db_name, host_value, user_name, password_value, port_value,date_time_stamp):
    list_to_insert = []
    for form_view in list_forms_views:
        list_to_insert.append([date_time_stamp.strftime('%Y-%m-%d %H:%M:%S'), application_name,form_view[0], form_view[1], form_view[2], form_view[3], form_view[4], form_view[5]])
    print(list_to_insert)
    conn = psycopg2.connect(dbname=db_name, host=host_value,
                            user=user_name, password=password_value, port=port_value)
    cur = conn.cursor()

    cur.executemany(
        "INSERT INTO forms_views_list (recorded_date_time, application_name, form_name, form_link_name, form_link_id, view_link_id, view_name, view_link_name) VALUES ( %s,%s, %s, %s, %s, %s, %s, %s);",
        list_to_insert)

    conn.commit()
    cur.close()
    conn.close()

# Get list of columns for the views

def get_view_columns (api_endpoint, api_data):
    view_columns = []
    r = requests.get(url=api_endpoint, params=api_data)

    api_output = r.text
    print(api_output)
    xml_tree_list = xmlTree.fromstring(api_output)
    for field in xml_tree_list.findall("./Fields"):
        view_columns.append([field.find('DisplayName').text, field.find('FieldName').text, field.find('apiType').text])

    return view_columns

#Update/add new columns if not found in table

def create_update_view_columns(application_link_name, view_link_name, view_columns, db_name, host_value, user_name, password_value, port_value):
    #check if table exists.



    try:
        conn = psycopg2.connect(dbname=db_name, host=host_value,
                                user=user_name, password=password_value, port=port_value)
        cur = conn.cursor()
        cur.execute("""CREATE table %s_X_%s (
                          application_name char(255),
                           application_link_name char(255),
                          form_name char(255),
                          form_link_name char(255),
                          view_name char(255),
                           view_link_name char(255),
                           starting_index int)
                      """% (application_link_name, view_link_name))

        conn.commit()
        cur.close()
        conn.close()
    except:
        print("Table exists")

    cur.close()
    conn.close()
    #get connecting table zoho to SQL tables
    conn2 = psycopg2.connect(dbname="client_list", host=host_value,
                            user=user_name, password=password_value, port=port_value)
    cur2 = conn2.cursor()
    cur2.execute("""SELECT * FROM zoho_sql_connector""")
    db_connectors = cur2.fetchall()
    cur2.close()
    conn2.close()

    for each_column_new in view_columns:

         for db_connector in db_connectors:

            if str(db_connector[1]) == str(each_column_new[2]):
                print(each_column_new[1])
                try:

                    conn = psycopg2.connect(dbname=db_name, host=host_value,
                                            user=user_name, password=password_value, port=port_value)
                    cur = conn.cursor()

                    cur.execute("""ALTER table %s_X_%s 
                                        ADD %s %s ;
                                          """ % (application_link_name, view_link_name,each_column_new[1],db_connector[2] ))

                    conn.commit()
                    cur.close()
                    conn.close()
                except Exception as e:
                    print(e)

#Get specific record from table

def get_record(api_endpoint, api_data, index_number):
    view_columns = []
    r = requests.get(url=api_endpoint, params=api_data)

    api_output = r.text
    print(api_output)
    xml_tree_list = xmlTree.fromstring(api_output)
    for field in xml_tree_list.findall("./Fields"):
        view_columns.append([field.find('DisplayName').text, field.find('FieldName').text, field.find('apiType').text])

    return view_columns

#Populate new records into the DB tables

def populate_record(forms_and_views_list, db_name, host_value, user_name, password_value, port_value):
    conn = psycopg2.connect(dbname=db_name, host=host_value,
                            user=user_name, password=password_value, port=port_value)
    cur = conn.cursor()

    cur.execute(""" 	SELECT * FROM client_details""")
    interface_que = cur.fetchall()
    cur.close()
    conn.close()
    return interface_que




#Main function to do a full update for all clients


if __name__ == '__main__':
    host_value = "jsonimport.c8ic7n39te9l.us-east-1.rds.amazonaws.com"
    user_name = "dbadmin"
    password_value = "K!u2Z(z0"
    port_value = 5432
    interface_que = que_sync_jobs("client_list", host_value, user_name, password_value, port_value)
    print(interface_que[0])
    now = datetime.datetime.now()
    print(now)

    for client_data in interface_que:
        auth_token = str(client_data[3]).strip()
        freq_value = client_data[2]
        db_name = str(client_data[1]).strip()
        zc_ownername = str(client_data[0]).strip()
        if 1:
        #if now.hour*2 % freq_value == 0:
            # check_client_existence in DB
            check_client_existence(db_name, host_value, user_name, password_value, port_value)
            # build APi endpoint and data
            api_endpoint = "https://creator.zoho.com/api/xml/applications"
            api_data = {'authtoken': auth_token,
                        'zc_ownername': zc_ownername,
                        'scope': 'creatorapi'}
            # get_application_list from API
            application_list = get_application_list(api_endpoint, api_data)
            print(application_list)

            # update_application_list on DB
            update_application_list(application_list, db_name, host_value, user_name, password_value, port_value, now)
            for app in application_list:

                #https://creator.zoho.com/api/<format>/<applicationName>/formsandviews
                api_endpoint = ("https://creator.zoho.com/api/xml/%s/formsandviews"% app[1])
                api_data = {'authtoken': auth_token,
                        'zc_ownername': zc_ownername,
                        'scope': 'creatorapi'}
                list_forms_views = get_forms_and_views(api_endpoint, api_data)
                update_forms_and_views(list_forms_views,app[1], db_name, host_value, user_name, password_value, port_value,now)
                for each_view_columns in list_forms_views:
                    #https://creator.zoho.com/api/<format>/<applicationName>/<formName>/fields
                    api_endpoint = ("https://creator.zoho.com/api/xml/%s/%s/fields"% (app[1], each_view_columns[1] ))
                    api_data = {'authtoken': auth_token,
                                'zc_ownername': zc_ownername,
                                'scope': 'creatorapi'}
                    view_columns = get_view_columns (api_endpoint, api_data)
                    create_update_view_columns(app[1],each_view_columns[1], view_columns, db_name, host_value, user_name, password_value, port_value)

                    #get_latest_record api (index)


                    #check latest and add if new



