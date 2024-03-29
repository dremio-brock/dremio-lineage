import requests
import json
import logging
import confuse
import urllib3

# Disable SSL warnings
urllib3.disable_warnings()


config = confuse.Configuration('DremioCredentialSync', __name__)
config.set_file('config.yaml', base_for_paths=True)

dremio_url = config['Dremio']['URL']
dremio_user = config['Dremio']['Username']
dremio_password = config['Dremio']['Password']
dremio_cloud = config['Dremio']['DremioCloud'].get(bool)
dremio_project = config['Dremio']['ProjectID']

# Load the log level from the configuration
log_level = config['logLevel'].get()

# Configure logging
logging.basicConfig(filename='debug.log', level=log_level)

lineage = []

def dremio_auth(url, user, password):
    logging.info('Authenticating with Dremio')

    url = "{url}/apiv2/login".format(url=url)

    payload = json.dumps({
        "userName": str(user),
        "password": str(password)
    })
    headers = {
        'Content-Type': 'application/json'
    }
    logging.debug('Authentication payload: %s', payload)  # Log the payload
    logging.debug('Authentication URL: %s', url)  # Log the URL
    
    response = requests.request("POST", url, headers=headers, data=payload, verify=False)
    
    if response.status_code != 200:
        logging.error('Authentication failed with Dremio')
        exit(0)
    else:
        token = response.json()['token']
        return token

def get_catalogs(token):
    logging.info('Fetching catalogs')
    if dremio_cloud is False:
        url = "{url}/api/v3/catalog".format(url=dremio_url)
    else:
        url = "{url}/projects/{project_id}/catalog".format(url=dremio_url, project_id=dremio_project)

    payload = {}
    headers = {
        'Authorization': token
    }
    logging.debug('Get Catalogs payload: %s', payload)  # Log the payload
    logging.debug('Get Catalogs URL: %s', url)  # Log the URL

    response = requests.request("GET", url, headers=headers, data=payload, verify=False).json()
    logging.debug('Catalogs: %s', response)
    return response['data']

def get_catalog_children(token, catalog_id):
    logging.info('Fetching catalog children')
    if dremio_cloud is False:
        url = "{url}/api/v3/catalog/{id}".format(url=dremio_url, id=catalog_id)
    else:
        url = "{url}/projects/{project_id}/catalog/{id}".format(url=dremio_url, id=catalog_id, project_id=dremio_project)

    payload = {}
    headers = {
        'Authorization': token
    }
    logging.debug('Get Catalog Children payload: %s', payload)  # Log the payload
    logging.debug('Get Catalog Children URL: %s', url)  # Log the URL

    response = requests.request("GET", url, headers=headers, data=payload, verify=False).json()
    logging.debug('Catalog Children: %s', response)
    return response

def get_graph(token, catalog_id):
    logging.info('Fetching catalog graph')
    if dremio_cloud is False:
        url = "{url}/api/v3/catalog/{id}/graph".format(url=dremio_url, id=catalog_id)
    else:
        url = "{url}/projects/{project_id}/catalog/{id}/graph".format(url=dremio_url, id=catalog_id, project_id=dremio_project)

    payload = {}
    headers = {
        'Authorization': token
    }
    logging.debug('Get Catalog Graph payload: %s', payload)  # Log the payload
    logging.debug('Get Catalog Graph URL: %s', url)  # Log the URL

    response = requests.request("GET", url, headers=headers, data=payload, verify=False).json()
    logging.debug('Catalog Graph: %s', response)
    return response

if __name__ == '__main__':
    if dremio_cloud is False:
        token = dremio_auth(dremio_url, dremio_user, dremio_password)
    else:
        token = "Bearer " + str(dremio_password)
    catalogs = get_catalogs(token)
    for catalog in catalogs:
        if 'containerType' in catalog:
            if catalog['containerType'] in ['HOME', 'SPACE']:
                if catalog['type'] == 'DATASET':
                    lineage.append(
                        {
                            'id': catalog['id'],
                            'name': catalog['path'][-1],
                            'path': '.'.join(f'"{path}"' for path in catalog['path']),
                            'data': catalog['id'],
                            'lineage': get_graph(token, catalog['id'])
                        }
                    )
                else:
                    child_catalogs = get_catalog_children(token, catalog['id'])
                    if 'children' in child_catalogs:
                        for child_catalog in child_catalogs['children']:
                            if child_catalog['type'] == 'DATASET':
                                lineage.append(
                                    {
                                        'id': child_catalog['id'],
                                        'name': child_catalog['path'][-1],
                                        'path': '.'.join(f'"{path}"' for path in child_catalog['path']),
                                        'data': child_catalog,
                                        'lineage': get_graph(token, child_catalog['id'])
                                    })
                            else:
                                catalogs.append(child_catalog)
    parent_list = []
    # Find PDS
    for catalog in lineage:
        for parent in catalog['lineage']['parents']:
            if parent['id'] not in catalog['id'] and parent['id'] not in parent_list:
                parent_list.append(parent['id'])
                parent_catalog = get_catalog_children(token, parent['id'])
                lineage.append(
                    {
                        'id': parent_catalog['id'],
                        'name': parent_catalog['path'][-1],
                        'path': '.'.join(f'"{path}"' for path in parent_catalog['path']),
                        'data': parent_catalog,
                        'lineage': get_graph(token, parent_catalog['id'])
                    })

    with open('lineage.json', 'w') as file:
        json.dump(lineage, file)
