import requests
import xml.etree.ElementTree as xmlTree
import dbInterface
	
	#Class to communicate with ZOHO API
	
def get_application_list(auth_token, zc_ownername):
    api_endpoint = "https://creator.zoho.com/api/xml/applications"
    api_data = {'authtoken': auth_token,
                'zc_ownername': zc_ownername,
                'scope': 'creatorapi'}

    # sending post request and saving response as response object
    r = requests.get(url=api_endpoint, params=api_data)

    # extracting response text
    api_output = r.text
    xml_tree_list = xmlTree.fromstring(api_output)
    application_list = []
    for app_list in xml_tree_list.findall("./result/application_list/applications/application"):
        application_list.append([(app_list.find('application_name').text),(app_list.find('link_name').text).replace("-","_")])
    return application_list

def get_forms_and_views(application_link_name,auth_token, zc_ownername):

    api_endpoint = ("https://creator.zoho.com/api/xml/%s/formsandviews" % application_link_name.replace("_","-"))
    api_data = {'authtoken': auth_token,
                'zc_ownername': zc_ownername,
                'scope': 'creatorapi'}
    list_forms = []
    list_forms_views = []
    # sending post request and saving response as response object
    r = requests.get(url=api_endpoint, params=api_data)
    print(application_link_name)
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

def get_view_columns (application_link_name, form_link_name, view_link_name,auth_token, zc_ownername):
    api_endpoint = ("https://creator.zoho.com/api/xml/%s/%s/fields" % (application_link_name.replace("_","-"), form_link_name))

    api_data = {'authtoken': auth_token,
                'zc_ownername': zc_ownername,
                'scope': 'creatorapi'}

    view_columns = []
    column_link_name = []
    r = requests.get(url=api_endpoint, params=api_data)

    api_output = r.text
    xml_tree_list = xmlTree.fromstring(api_output)
    for field in xml_tree_list.findall("./Fields"):

        view_columns.append([field.find('DisplayName').text, field.find('FieldName').text.replace(".","_o_"), field.find('apiType').text])
        column_link_name.append(field.find('FieldName').text)


    api_endpoint = ("https://creator.zoho.com/api/xml/%s/view/%s" % (application_link_name.replace("_","-"), view_link_name))


    api_data = {'authtoken': auth_token,
                'zc_ownername': zc_ownername,
                'scope': 'creatorapi'}

    r = requests.get(url=api_endpoint, params=api_data)

    api_output = r.text

    xml_tree_list = xmlTree.fromstring(api_output)

    try:
        for record in xml_tree_list.find("./records"):
            for column in record.findall("./"):
                for field in column.findall("./"):
                    if any(column.attrib["name"].lower() in s.lower() for s in column_link_name):
                        pass
                    else:

                        view_columns.append([column.attrib["name"],column.attrib["name"].replace(".","_o_"), 1])
    except Exception as e:
        print("This will fail if table doesn't exist")
        print(e)

    return view_columns


def get_new_records(application_name, application_link_name,form_name, form_link_name,
                     view_name, view_link_name, auth_token, zc_ownername, db_name):
    #https://creator.zoho.com/api/<format>/<applicationLinkName>/view/<viewLinkName>
    api_endpoint = ("https://creator.zoho.com/api/xml/%s/view/%s" % (application_link_name.replace("_","-"), view_link_name))
    return_values = []

    api_data = {'authtoken': auth_token,
                'zc_ownername': zc_ownername,
                'scope': 'creatorapi'}

    r = requests.get(url=api_endpoint, params=api_data)

    api_output = r.text
    print(api_output)

    xml_tree_list = xmlTree.fromstring(api_output)

    for record in xml_tree_list.findall("./records/"):

        if dbInterface.check_record(application_link_name,view_link_name, db_name, record.attrib["id"]):
            break



        column_list = []
        value_list = []
        column_list.append("record_id")
        column_list.append("application_name")
        column_list.append("application_link_name")
        column_list.append("form_name")
        column_list.append("form_link_name")
        column_list.append("view_name")
        column_list.append("view_link_name")


        value_list.append(record.attrib["id"])
        value_list.append(application_name)
        value_list.append(application_link_name)
        value_list.append(form_name)
        value_list.append(form_link_name)
        value_list.append(view_name)
        value_list.append(view_link_name)


        for column in record.findall("./"):

            for field in column.findall("./"):

                column_list.append(column.attrib["name"])
                value_list.append(field.text)

        return_column_list = str((', '.join('{0}'.format(w) for w in column_list))).replace(".","_o_")
        return_value_list = str((', '.join("'{0}'".format(w) for w in value_list))).replace("'None'","null")

        return_values.append([return_column_list,return_value_list])

    return return_values

