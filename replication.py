import requests
from requests.auth import HTTPBasicAuth
import json
import base64

db = 'mydb'
source = 'softfactors'
source_auth = HTTPBasicAuth(source, 'mypassword')

target = 'softfactors-lon'
target_auth = HTTPBasicAuth(target, 'myotherpassword')


def check_credentials(account, auth):
    r = requests.get('https://{0}.cloudant.com/_all_dbs?limit=1'.format(account),
        auth=auth)

    if r.status_code is not 200:
        print "Failed to access account {0}:".format(account)
        print "{0}: {1}".format(r.status_code, r.text)
        return False

    return True

def create_db_if_not_exists(account, auth, db):
    requests.put('https://{0}.cloudant.com/{1}'.format(account, db),
        auth=auth)


def create_API_key(account, auth):
    r = requests.post('https://{0}.cloudant.com/_api/v2/api_keys'.format(account),
    auth=auth)
    return r.json()


def assign_admin_permissions(account, auth, db, api_key):
    r = requests.get('https://{0}.cloudant.com/_api/v2/db/{1}/_security'.format(account, db),
    auth=auth)
    security_obj = r.json()

    if "cloudant" not in security_obj:
        security_obj["cloudant"] = {}

    security_obj["cloudant"][api_key["key"]] = ["_reader", "_writer", "_admin"]
    requests.put('https://{0}.cloudant.com/_api/v2/db/{1}/_security'.format(account, db),
    auth=auth,
    headers={'Content-Type': 'application/json'},
    data=json.dumps(security_obj))


def configure_push_replication(account, auth, to, db, api_key):
    create_db_if_not_exists(account, auth, '_replicator')
    from_url = 'https://{0}.cloudant.com/{1}'.format(account, db)
    to_url = 'https://{0}.cloudant.com/{1}'.format(to, db)
    api_key_auth = "Basic {0}".format(base64.b64encode("{0}:{1}".format(api_key["key"], api_key["password"])))
    replication_doc_id = "{0}-{1}-{2}".format(account, to, db)

    r = requests.post('https://{0}.cloudant.com/_replicator'.format(account),
        auth=auth,
        headers={'Content-Type': 'application/json'},
        data=json.dumps({
            "_id": replication_doc_id,
            "source": {
                "url": from_url,
                "headers": {
                    "Authorization": api_key_auth
                }
            },
            "target": {
                "url": to_url,
                "headers": {
                    "Authorization": api_key_auth
                }
            },
            "continuous": True
        })
    )

    if r.status_code is 201:
        print ""
    else:
        print "Failed to create replication doc https://{0}.cloudant.com/_replicator/{1}:".format(account, replication_doc_id)
        print "{0}: {1}".format(r.status_code, r.text)


if check_credentials(source, source_auth) and check_credentials(target, target_auth):
    create_db_if_not_exists(target, target_auth, db)
    api_key = create_API_key(target, target_auth)
    assign_admin_permissions(target, target_auth, db, api_key)
    assign_admin_permissions(source, source_auth, db, api_key)
    configure_push_replication(source, source_auth, target, db, api_key)
    configure_push_replication(target, target_auth, source, db, api_key)
