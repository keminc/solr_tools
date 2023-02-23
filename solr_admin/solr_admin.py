#! /usr/bin/env python
# -*- coding: utf-8 -*-
# by Kotov E.
# version: v1.0 01.03.2022
# version: v1.1 03.03.2022
# version: v1.2 25.03.2022
# version: v1.3 24.05.2022
#           Add httpS + Auth
#           Add del coll. from alias
#           Add job id check (action_check_job)
# version: v1.4 - 10.06.2022
#            fix add collection to alias errorPPRB
#            human readable json config
#
# version: v1.5 - 21.10.2022
#            add HTTPS to solt and use certs
#            add individual cluster "last_async"
#
# version: v1.6 - 13.11.2022
#            add move replicas
#            add delete replicas dublicate (>2)
#
#############################################################
#"C:\Program Files\Python39\Scripts\pip.exe" install --index-url="http://mirror/pypi/simple/" pip --upgrade --trusted-host mirror --user
#install requests
#"C:\Program Files\Python39\Scripts\pip.exe" install --index-url="http://mirror/pypi/simple/" requests --user --trusted-host mirror
#install local package
#"C:\Program Files\Python39\Scripts\pip.exe" install --index-url="http://mirror/pypi/simple/" "C:\Distr\python\grafana_api-1.0.3" --user --trusted-host mirror
#############################################################

import requests, urllib3, base64, json, re, time, os, sys
from base64 import b64encode
from requests.auth import HTTPBasicAuth
from datetime import datetime
from random import randrange

def get_config(config_file='solr_admin.conf'):
    try:
        with open(config_file, 'r') as file:
            return json.load(file)
    except Exception as e:
        add_to_log('Error when get config: '+config_file+'. Error: ' + str(e))
        return False

def set_config(conf_json, config_file='solr_admin.conf'):
    try:
        with open(config_file, 'w') as file:
            return json.dump(conf_json,
                             file,
                             sort_keys=True,
                             indent=4,
                             separators=(',',': '))
    except Exception as e:
        add_to_log('Error when set config: '+config_file+'. Error: ' + str(e))
        exit(125)

########################################################################################################################
def add_to_log(datastr,PRINT_LOG_DATA='YES'):
    now = datetime.now()
    now = now.strftime('%Y-%m-%d %H:%M:%S')
    pwd = os.getcwd()
    with open(os.path.join('log',"solr_admin_"+SolrCluster+".txt"), "a") as logfile:
        txt = str(now +'\t'+datastr)
        logfile.write(txt + '\n')
        logfile.close()
    # if re.search('.*error.*', datastr, re.IGNORECASE):
    #     add_log_kafka(errorlevel='logdata',
    #                 cluster=stend,
    #                  message=str(datastr))

    if (PRINT_LOG_DATA == 'YES'):
        print(datastr)
    return 0

#######################################################################################################################
# R E Q U E S T S
def getRequest(URL, command='', timeout=5):
    try:
        #auth = (druid_role_conf["druid_admin_user"], druid_role_conf["druid_admin_pwd"])
        if URL.split('://')[0] == 'https':
            if re.search('PSI', SolrCluster):
                certs = (os.path.join('ssl','mycert.pem'), os.path.join('ssl','mycert.key'))
            else:
                certs = (os.path.join('ssl', 'mycert2.pem'), os.path.join('ssl', 'mycert2.key'))

            response = requests.get(URL + command,
                                    timeout=timeout,
                                    auth=HTTPBasicAuth(solr_Auth["user"], solr_Auth["pwd"]),
                                    verify=False,
                                    cert=certs)
        else:
            response = requests.get(URL + command,
                                    timeout=timeout)

        if response.status_code == 200:
            add_to_log('\tResponse: ' + str(response.status_code), 'No')
            return True, response
        else:
            add_to_log('\tResponse error: ' + str(response.reason), 'YES')
            return False, response #.json()['error']
    except Exception as e:
        add_to_log('\tResponse Exception:' + str(e))
        return False, str(e)

########################################################################################################################
def test_solr(URL):
    res, response = getRequest(URL, '/solr/admin/collections?action=LISTALIASES')
    if res:
        add_to_log('Solr web on SolrURL - online: ' + URL)
        return True
    else:
        try:
            errmsg = str(response.reason)
        except:
            errmsg = str(response)
        add_to_log('Solr web on SolrURL - offline [' + errmsg + ']: ' + URL)
        return False


########################################################################################################################
def get_aliases(URL, regexp='.*'):
    #response = requests.get(URL+'/solr/admin/collections?action=LISTALIASES', timeout=10)
    res, response = getRequest(URL, '/solr/admin/collections?action=LISTALIASES')
    if not res:
        add_to_log('\t\tSolr get LISTALIASES failed '+ response.reason +' from: ' + URL)
        return False


    add_to_log('\t\tSolr get LISTALIASES successed.')
    ClusterALIASES = response.json()
    SolrAliases = []
    for alias in ClusterALIASES['aliases']:
        SolrAlias = {}
        if re.search(regexp, alias):
            SolrAlias['name'] = alias
            SolrAlias['collection'] = ClusterALIASES['aliases'][alias]
            SolrAliases.append(SolrAlias)
    return SolrAliases


########################################################################################################################
def action_alias(action, URL, alias, collections=''):
    if action == 'CREATEALIAS':
        action_url = 'CREATEALIAS&name=' + alias + '&collections=' + collections
    elif action == 'DELETEALIAS':
        action_url = 'DELETEALIAS&name=' + alias
    #http://mysolr:8983/solr/admin/collections?action=CREATEALIAS&name=SYNAPSE_WRITE&collections=synapse-2021-05-12
    #response = requests.get(URL + '/solr/admin/collections?action=' + action_url, timeout=30)
    res, response = getRequest(URL, '/solr/admin/collections?action=' + action_url, timeout=30)
    if res:
        add_to_log('\t\t\tSolr action '+action+' "'+alias+'" successed.', 'YES')
        return True
    else:
        try:
            errmsg = str(response.json()['exception']['msg'])
        except:
            errmsg = str(response)
        add_to_log('\t\t\tSolr action '+action+' failed on: ' + URL +'. Error: '+ errmsg, 'YES')
        return False



########################################################################################################################
def action_alias_rename(URL, from_alias, to_alias):
    ALIASES = get_aliases(host, '^'+from_alias+'$')
    coll_list=''
    if not ALIASES:
        print("\tAliases not found.")
        return False
    else:
        for al in ALIASES:
            coll_list=al['collection']+','
        coll_list=coll_list[:len(coll_list)-1]

        if action_alias(action='CREATEALIAS', URL=URL, alias=to_alias, collections=coll_list) and action_alias(action='DELETEALIAS', URL=URL, alias=from_alias):
            add_to_log('\t\tRename alias from '+from_alias +' to '+ to_alias+ ' successed.','YES')
            return  True
        else:
            add_to_log('\t\tRename alias from ' + from_alias + ' to ' + to_alias + ' failed.', 'YES')
            return False
########################################################################################################################
def action_jobid_check(URL, anync):
    RURL = URL + '/solr/admin/collections?action=REQUESTSTATUS&wt=json&requestid=' + str(anync)
    res, response = getRequest(RURL, timeout=5)
    if res:
        jobStatus = response.json()
        status = jobStatus['status']['state']
        return status
    else:
        return False

def action_jobid_getfree(URL, anync):
    i = 0
    status = action_jobid_check(URL, anync)
    while status != 'notfound':
        i += 1
        if i > 500:
            add_to_log('Async job id [' +str(anync)+ '] is to low. Update job id to hight value or reset it in Solr.')
            set_config(ClustersConfigs)
            return False, ''
        anync += + 1
        status = action_jobid_check(URL, anync)

    ClustersConfigs['Clusters'][SolrCluster]['last_async'] = anync
    set_config(ClustersConfigs)
    return True, status

########################################################################################################################
def action_whait_anync(URL, anync):
    RunJob = True
    RURL = URL + '/solr/admin/collections?action=REQUESTSTATUS&wt=json&requestid=' + str(anync)
    print('\t\t\tJob status URL: ' + RURL)
    print('\t\t\tWait while request finishing.', end='')
    count = 0
    time.sleep(2)

    while RunJob:
        try:
            #response = requests.get(RURL, timeout=20)
            res, response = getRequest(RURL,  timeout=20)
            jobStatus = response.json()
            status = jobStatus['status']['state']
            count += 1
            if response.status_code == 200:
                if status in ('notfound'):
                    RunJob = False
                    add_to_log('\n\t\t\tSolr REQUESTSTATUS job: ' + str(anync) + ' not found: ' )

                elif status == 'failed':
                    RunJob = False
                    etxt = ''
                    if jobStatus.get('exception', False):
                        etxt = jobStatus['exception']['msg']
                        if len(etxt) < 5:
                            etxt = jobStatus['failure']
                    add_to_log('\n\t\t\t\tSolr REQUESTSTATUS job: ' + str(anync) + ' failed: ' + etxt)

                elif status in ('running', 'submitted'):
                    if ClustersConfigs['timeout_sec'] - (count*5) > 0:
                        print('.', end='')
                        sys.stdout.flush()
                        time.sleep(5)
                    else:
                        add_to_log('\n\t\t\t\tTimeout wait for finish job. Go f..k youself. ')
                        RunJob = False

                elif status == 'completed':
                    RunJob = False
                    add_to_log('\n\t\t\t\tSolr REQUESTSTATUS job: ' + str(anync) + ' successed.')
                    return True
                else:
                    RunJob = False
                    add_to_log('\n\t\t\t\tSolr REQUESTSTATUS job: ' + str(anync) + ' unknown state from: ' + jobStatus['status']['msg'])

            else:
                RunJob = False
                add_to_log('\n\t\t\t\tSolr get REQUESTSTATUS failed job:' + str(anync) + '. Error: ' + jobStatus['status']['msg'])

        except Exception as e:
            add_to_log('\n\t\t\t\tSolr get REQUESTSTATUS failed job:' + str(anync) + '. Error: ' + str(e))
            return False

    return RunJob


def action_alias_add_collection(URL, alias, collection):
    ALIASES = get_aliases(host, '^' + alias + '$')

    if not ALIASES:
        print("\tAliases not found.")
        return False
    else:
        if len(ALIASES) > 1:
            print("\tFound more than one alias. Failed.")
            return False
        for al in ALIASES:
            coll_list = al['collection'] + ','+collection

            if      action_alias(action='CREATEALIAS', URL=URL, alias=al['name']+'_new', collections=coll_list) and \
                    action_alias(action='DELETEALIAS', URL=URL, alias=al['name']) and\
                    action_alias_rename(URL, from_alias=al['name']+'_new', to_alias=al['name']):
                return True
            else:
                add_to_log('\t\tError when add collection: "' + collection + '" to alias: "' + alias + '"', 'YES')
                return False


########################################################################################################################
def action_alias_collection_delete(URL, collection, alias='.*'):
    #########
    # get aliases where "collection" is in and remove "collection" from list
    aliases_list = {}
    result = True
    ALIASES = get_aliases(URL, regexp=alias)
    if not ALIASES:
        print("\tAliases not found.")
        return False
    else:
        # Search collection in all aliases
        for al in ALIASES:
            if re.search('(^|,| )' + collection + '($|,| )', al['collection']):
                col_list = re.sub('( )', '', al['collection'])
                col_list = re.sub('(^|,)(' + collection + ')($|,)', '\\3', col_list)  # del collection name
                col_list = re.sub('(^,)', '', col_list)
                aliases_list[al['name']] = col_list
                if len(col_list) < 3:
                    add_to_log('\t\tError on step del collection: "' + collection + '" from alias: "' + al['name'] +
                               '". Collection is only one in Alias. Delete alias first, if you need.','YES');
                    result = result and False
                else:
                    #########
                    # delete collection from alias
                    if action_alias_rename(URL, from_alias=al['name'], to_alias=al['name'] + '-tmp') and \
                            action_alias(action='CREATEALIAS', URL=URL, alias=al['name'], collections=col_list) and \
                            action_alias(action='DELETEALIAS', URL=URL, alias=al['name'] + '-tmp'):
                        result = result and True
                    else:
                        add_to_log('\t\tError on step del collection: "' + collection + '" from alias: "' + al['name'] + '"',
                                    'YES');
                        result = result and False
        return result

########################################################################################################################
def action_collection_delete(URL, collection):
        #########
        # get aliases where "collection" is in and remove "collection" from list
        if not action_alias_collection_delete(URL, collection, alias='.*'):
            return False
        #########
        # delete collection
        try:
            ClustersConfigs['Clusters'][SolrCluster]['last_async'] = ClustersConfigs['Clusters'][SolrCluster]['last_async'] + 1
            set_config(ClustersConfigs)
            #response = requests.get(URL + '/solr/admin/collections?action=DELETE&name='+collection+'&async='+str(config['last_async']), timeout=20)
            res, response = getRequest(URL, '/solr/admin/collections?action=DELETE&name='+collection
                                       + '&async=' + str(ClustersConfigs["Clusters"][SolrCluster]['last_async']),
                                       timeout=20)
            status = response.json()['responseHeader']
            if response.status_code == 200:
                #whait when async request will finished
                if status['status'] == 0:
                   add_to_log('\t\tSolr DELETE collection: ' + collection + ' by async job: '+ str(ClustersConfigs['Clusters'][SolrCluster]['last_async']))
                   if  action_whait_anync(URL, ClustersConfigs["Clusters"][SolrCluster]['last_async']) :
                       add_to_log('\t\t\tSolr DELETE collection: '+collection+' successed.')
                   else:
                       add_to_log('\t\t\tSolr DELETE collection: ' + collection + ' failed from: ' + URL)
            else:
                add_to_log('\t\t\tSolr DELETE collection failed from: ' + URL + '. Msg: ' + status['status']['msg'] )
                return False

        except Exception as e:
            add_to_log('\t\t\tSolr DELETE collection failed from: ' + URL + ' Error: ' + str(e))
            return False

########################################################################################################################
def get_cluster_status(URL):
    #Core size

    try:
        add_to_log('\t\t# Get CLUSTERSTATUS from server', 'NO')
        #response = requests.get(URL+'/solr/admin/collections?action=CLUSTERSTATUS&indent=on', timeout=60)
        res, response = getRequest(URL, '/solr/admin/collections?action=CLUSTERSTATUS&indent=on', timeout=30)
        cluster_status = response.json()
        if response.status_code == 200:
            return True, cluster_status
        else:
            return False, cluster_status['status']['msg']
    except Exception as e:
        add_to_log('\t\tError in function: get_cluster_status on step: collections?action=CLUSTERSTATUS for server: '
                   + URL + ' Error:' + str(e))
        return False, ''
########################################################################################################################
def show_host_collections(URL, collection, host):
    return 0

########################################################################################################################
def action_collection_delete_not_active_replica(URL, collection, shard, replica):
    # http://mysolr:8983/solr/admin/collections?action=DELETEREPLICA&collection=synapse-2021-05-04&shard=shard97&replica=core_node410&async=1103

    try:
        action_jobid_getfree(URL, ClustersConfigs["Clusters"][SolrCluster]['last_async'])

        # response = requests.get(
        #     URL + '/solr/admin/collections?action=DELETEREPLICA&collection=' + collection +'&shard='+ shard +
        #             '&replica=' +replica+'&async=' + str(ClustersConfigs["Clusters"][SolrCluster]['last_async']),
        #     timeout=20)
        res, response = getRequest(URL, '/solr/admin/collections?action=DELETEREPLICA&collection=' + collection +
                                        '&shard=' + shard +
                                        '&replica=' +replica+
                                        '&async=' + str(ClustersConfigs["Clusters"][SolrCluster]['last_async']),
                                        timeout=20)

        status = response.json()['responseHeader']
        if response.status_code == 200:
            # whait when async request will finished
            if status['status'] == 0:
                add_to_log('\t\t# Solr DELETEREPLICA in collection: ' + collection + ' by async job: ' + str(ClustersConfigs["Clusters"][SolrCluster]['last_async']))
                if action_whait_anync(URL, ClustersConfigs["Clusters"][SolrCluster]['last_async']):
                    add_to_log('\t\t# Solr DELETEREPLICA in collection: ' + collection + ' successed.')
                    return True
                else:
                    add_to_log('\t\t# Solr DELETEREPLICA in collection: ' + collection + ' failed from: ' + URL)
                    return False
        else:
            add_to_log('\t\t# Solr DELETEREPLICA in collection failed from: ' + URL + '. Msg: ' + status['status']['msg'])
            return False

    except Exception as e:
        add_to_log('\t\t# Solr DELETEREPLICA in collection failed from: ' + URL + ' Error: ' + str(e))
        return False

    return True
########################################################################################################################
def action_collection_move_replicas(URL, from_host, collection='.*', shard='.*', to_host='.*'):
    result, cluster_status = get_cluster_status(URL)
    nodes = cluster_status["cluster"]["live_nodes"]
    if len(nodes) == 0:
        print('\t\t# Error: no live nodes.')
        return False

    if result:
        for coll in cluster_status["cluster"]["collections"]:
            if re.search(collection, coll):
                for sh in cluster_status["cluster"]["collections"][coll]["shards"]:
                    if re.search(shard, sh):
                        for repl in cluster_status["cluster"]["collections"][coll]["shards"][sh]["replicas"]:
                            node = cluster_status["cluster"]["collections"][coll]["shards"][sh]["replicas"][repl]["node_name"]
                            if re.search(from_host, node):
                                replica = cluster_status["cluster"]["collections"][coll]["shards"][sh]["replicas"][repl]
                                if (replica.get("state", '') == "active") :
                                    #select node
                                    target_node = from_host
                                    c = 0
                                    while (not re.search(to_host, target_node)) or c == 500:
                                        c = c + 1
                                        target_node = nodes[randrange(len(nodes))]
                                    if c == 500:
                                        print("\t\t# Move replica: (" + coll + " " + repl + '). Cannt find good nodes ')
                                        continue

                                    #Move
                                    try:
                                        action_jobid_getfree(URL,
                                                             ClustersConfigs["Clusters"][SolrCluster]['last_async'])
                                        res, response = getRequest(URL,
                                                                   '/solr/admin/collections?action=MOVEREPLICA'
                                                                   '&collection=' + coll +
                                                                   '&shard=' + sh +
                                                                   '&replica=' + repl +
                                                                   '&sourceNode=' + node +
                                                                   '&targetNode=' + target_node +
                                                                   '&async=' + str(ClustersConfigs["Clusters"][SolrCluster]['last_async']),
                                                                   timeout=20)
                                        status = response.json()['responseHeader']
                                        if response.status_code == 200:
                                            # whait when async request will finished
                                            if status['status'] == 0:
                                                add_to_log('\t\tSolr MOVEREPLICA in collection: ' + coll + ' ' + sh +'. '
                                                           + node + ' -> ' + target_node + ' by async job: ' + str(ClustersConfigs["Clusters"][SolrCluster]['last_async']))
                                                time.sleep(3)
                                                if action_whait_anync(URL, ClustersConfigs["Clusters"][SolrCluster]['last_async']):
                                                    add_to_log('\t\tSolr MOVEREPLICA in collection: ' + coll + ' successed.')
                                                else:
                                                    add_to_log('\t\tSolr MOVEREPLICA in collection: ' + coll + ' failed from: ' + URL)
                                        else:
                                            add_to_log('\t\tSolr MOVEREPLICA in collection failed from: ' + URL + '. Msg: ' + status['status']['msg'])

                                    except Exception as e:
                                        add_to_log(
                                        '\t\tSolr ADDREPLICA in collection failed from: ' + URL + ' Error: ' + str(e))


########################################################################################################################
def action_collection_delete_not_active_replicas(URL, collection='.*', shard='.*', host='.*', addrelpica=True, addReplicaIfNotExist=False, delrecoveryrelpica=False):
    result, cluster_status = get_cluster_status(URL)
    # result = True
    # with open('CLUSTERSTATUS.json','r') as file:
    #     cluster_status = json.load(file)

    if result:
        for coll in cluster_status["cluster"]["collections"]:
            if re.search(collection, coll):
                for sh in cluster_status["cluster"]["collections"][coll]["shards"]:
                    if re.search(shard, sh):
                        for repl in cluster_status["cluster"]["collections"][coll]["shards"][sh]["replicas"]:
                            if re.search(host, cluster_status["cluster"]["collections"][coll]["shards"][sh]["replicas"][repl]["node_name"]):
                                replica = cluster_status["cluster"]["collections"][coll]["shards"][sh]["replicas"][repl]

                                #Create replica in it one
                                if addReplicaIfNotExist:
                                    if replica.get("state", '') == "active" \
                                            and replica.get('leader', True) \
                                            and len(cluster_status["cluster"]["collections"][coll]["shards"][sh]["replicas"]) < 2:
                                        action_collection_create_replicas(URL, coll, sh, 1)
                                    continue
                                #########################


                                if (replica.get("state", '') != "active"
                                        and (replica.get("state", '') != "recovering" or delrecoveryrelpica))\
                                    and replica.get('leader', False) == False:

                                    #check replica count
                                    if len(cluster_status["cluster"]["collections"][coll]["shards"][sh]["replicas"]) < 2:
                                        print("\t\t# Replica: (" + coll + " shard: " + sh + ')  state: ' + replica.get("state", '') + ' ONE replica. Cant delete.')
                                        continue

                                    print("\t\t# Replica: (" + coll + "-" + repl + ') state: ' + replica.get("state", ''))
                                    action_collection_delete_not_active_replica(URL, coll, sh, repl)
                                    if addrelpica:
                                        action_collection_create_replicas(URL, coll, sh, 1)

    else:
        return False

########################################################################################################################
def get_collections_with_replicas(URL, max_replicas = 2, params=[]):
    result, cluster_status = get_cluster_status(URL)

    if result:
        collections = cluster_status["cluster"]["collections"]
        for collection in collections:
            shards =  collections[collection]["shards"]
            for shard in shards:
                replicas = shards[shard]["replicas"]
                if len(replicas) > max_replicas:
                    r_leader = 0
                    r_active = 0
                    r_down = 0
                    r_recovery = 0
                    print("\t\tCollection: " + collection + ' shard: ' + shard + ' replicas: ' + str(len(replicas)))
                    for replica in replicas:
                        if replicas[replica]["state"] == "active":
                            r_active += 1
                        if replicas[replica]["state"] == "down":
                            r_down += 1
                        if replicas[replica]["state"] == "recovering":
                            r_recovery += 1
                        if replicas[replica].get('leader', False):
                            r_leader += 1


                        # Delete action
                        if params.get('host', '') != '':
                            if re.search(params.get('host', 'nohost'), replicas[replica]["node_name"]) \
                                    and re.search(params.get('collection', 'nocoll'), collection) \
                                    and re.search(params.get('status', 'nostate'), replicas[replica]["state"]):
                                if (not replicas[replica].get('leader', False)) or params['host'] != '.*':
                                    action_collection_delete_not_active_replica(URL, collection=collection, shard=shard, replica=replica)

                                    break
                        #####
                    #print("\t\t\tActive: " + str(r_active) + '. Down: ' + str(r_down) + '. Leader: ' + str(r_leader) + '. Recovery: ' + str(r_recovery))







        return True
    else:
        return False

########################################################################################################################
def action_collection_create_replicas(URL, collection,shard, count):
    result, cluster_status = get_cluster_status(URL)

    if result:
        nodes = cluster_status["cluster"]["live_nodes"]
        if len(nodes) == 0:
            print('\t\t# Error: no live nodes.')
            return False

        for i in range(int(count)):
            try:
                action_jobid_getfree(URL, ClustersConfigs["Clusters"][SolrCluster]['last_async'])
                rnode = nodes[randrange(len(nodes))]
                # response = requests.get(
                #     URL + '/solr/admin/collections?action=ADDREPLICA&collection=' + collection +'&shard='+ shard +
                #             '&node=' + rnode +'&async=' + str(ClustersConfigs["Clusters"][SolrCluster]['last_async']),
                #     timeout=20)

                res, response = getRequest(URL,'/solr/admin/collections?action=ADDREPLICA&collection=' + collection +
                                           '&shard='+ shard +
                                           '&node=' + rnode +
                                           '&async=' + str(ClustersConfigs["Clusters"][SolrCluster]['last_async']),
                                             timeout=20)
                status = response.json()['responseHeader']
                if response.status_code == 200:
                    # whait when async request will finished
                    if status['status'] == 0:
                        add_to_log('\t\tSolr ADDREPLICA in collection: ' + collection +' on '+ rnode +' by async job: ' + str(ClustersConfigs["Clusters"][SolrCluster]['last_async']))
                        if action_whait_anync(URL, ClustersConfigs["Clusters"][SolrCluster]['last_async']):
                            add_to_log('\t\tSolr ADDREPLICA in collection: ' + collection + ' successed.')
                        else:
                            add_to_log('\t\tSolr ADDREPLICA in collection: ' + collection + ' failed from: ' + URL)
                else:
                    add_to_log('\t\tSolr ADDREPLICA in collection failed from: ' + URL + '. Msg: ' + status['status']['msg'])

            except Exception as e:
                add_to_log('\t\tSolr ADDREPLICA in collection failed from: ' + URL + ' Error: ' + str(e))

        return  True
    else:
        return False


########################################################################################################################
def menu_select_cluster(NameList):
    clusternamelist = ''
    for clustername in NameList:
        clusternamelist += clustername + ' '
    print("Select cluster: ", clusternamelist)
    clustername = input('# Cluster (PSI): ')
    if clustername == '':   clustername = 'PSI'

    return clustername
########################################################################################################################
def menu_action_data(json_data):
    for param in json_data:
        #print("Input value for ", clusternamelist,": " , end='')
        usrval = input("Input value for: " + param + " [def: \"" + json_data[param] + "\"]: ")
        if usrval != '':
            json_data[param] = usrval
    return json_data
########################################################################################################################
def menu_select_action():
    print("Select action:")
    print("  ## View:")
    for a in actions_list:
        if not re.match('[A-z]',a):
            print("     ", a + ".", actions_list[a])
        else:
            print("     ", actions_list[a])

    params = {}
    action = input('# Action (1): ')
    if action == '':
        action = '1'

    if actions_list[action] == "List alias":
        params['pattern'] = input("  Search pattern (.*): ")
        if params['pattern'] == '':   params['pattern'] = '.*'
    elif actions_list[action] == 'List/Del collections (replicas > 2)':
        params['max_replicas'] = '2'
        if input("    Delete replicas more 2 repl.? (n) ") == 'y':
            params['host'] = input("   Host (.*):")
            params['collection'] = input("   Collection (.*):")
            params['status'] = input("   Status (.*):")
            if params['host'] == '':   params['host'] = '.*'
            if params['collection'] == '':   params['collection'] = '.*'
            if params['status'] == '':   params['status'] = '.*'

    elif actions_list[action] == "Create alias":
        params['alias_name'] = input("  Alias name: ")
        params['coll_list'] = input("  Collection list (ex. c1,c2,c3): ")
    elif actions_list[action] == "Rename alias":
        params['alias_name'] = input("  From alias: ")
        params['to_alias'] = input("  To alias: ")
    elif actions_list[action] == "Add collection to alias":
        params['alias_name'] = input("  Alias: ")
        params['coll_name'] = input("  Collections (ex. c1,c2): ")
    elif actions_list[action] == "Delete collection from alias":
        params['alias_name'] = input("  Alias (.*): ")
        if params['alias_name'] == '':   params['alias_name'] = '.*'
        params['coll_name'] = input("  Collection : ")
    elif actions_list[action] == "Delete alias":
        params['alias_name'] = input("  Alias name: ")
    elif actions_list[action] == "Delete collection":
        params['collection'] = input("  Collection name: ")
    elif (actions_list[action] == "Delete dead replicas") or (actions_list[action] == 'Add replica'):
        print("  Delete not Active & not Leader replicas.")
        params['collection'] = input("   Collection (.*):")
        params['shard'] = input("   Shard (.*): ")
        params['host'] = input("   Host (.*): ")
        params['addreplica'] = input("   Add new replica (y): ")
        params['delrecoveryrelpica']  = input("   Del recovery replica (y): ")

        if params['collection'] == '':   params['collection'] = '.*'
        if params['shard'] == '':   params['shard'] = '.*'
        if params['host'] == '':   params['host'] = '.*'
        if params['addreplica'] in ('','y'):
            params['addreplica'] = True
        else:
            params['addreplica'] = False
        if params['delrecoveryrelpica'] in ('','y'):
            params['delrecoveryrelpica'] = True
        else:
            params['delrecoveryrelpica'] = False

    elif (actions_list[action] ==  "Move replica"):
        params['collection'] = input("   Collection (.*):")
        params['shard'] = input("   Shard (.*): ")
        params['from_host'] = input("   From host (.*name.*): ")
        params['to_host'] = input("   To host (.*name.*): ")
        if params['collection'] == '':   params['collection'] = '.*'
        if params['shard'] == '':   params['shard'] = '.*'
        if params['from_host'] == '':   params['host'] = '.*name.*'
        if params['to_host'] == '':   params['host'] = '.*name2.*'
    elif (actions_list[action] == "Show host collections"):
        params['host'] = input("   Host (.*):")
        params['collection'] = input("   Collection (.*):")
        params['status'] = input("   Status (.*):")
        if params['host'] == '':   params['host'] = '.*'
        if params['collection'] == '':   params['collection'] = '.*'
        if params['status'] == '':   params['status'] = '.*'


    elif actions_list[action] == "Exit":
        params={}
    else:
        return False, action

    for param in params:
        if params[param] == '' :
            return False, ''

    return actions_list.get(action, False), params

def password_encode(str):
    str = base64.b16encode(str.encode('utf-8'))
    str = str.decode('utf-8')
    return str

def password_decode(str):
    str = str.encode('utf-8')
    str = base64.b16decode(str)
    str = str.decode('utf-8')
    return str
##################################################
# MAIN
##################################################

SolrCluster = ''
ClustersConfigs = get_config()
if not ClustersConfigs:
    exit(123)

solr_Auth = False
#solr_Auth = get_config('solr_last_auth.conf')
if not solr_Auth:
    solr_Auth = {"user": "admin",
                 "pwd": ""}
else:
    solr_Auth["pwd"] = password_decode(solr_Auth["pwd"])


actions_list={

                   "A": "----- ALIAS -----",
                   "1": "List alias",
                   "2": "Create alias",
                   "3": "Rename alias",
                   "4": "Delete alias",
                   "6": "Add collection to alias",
                   "7": "Delete collection from alias",
                   "C": "----- COLLECTIONS -----",
                   "8": "Add replica",
                   "9": "Move replica",
                   "10": "Delete dead replicas",
                   "11": "Delete collection",
                   "12": "Show host collections",
                   "D": "----- OTHER -----",
                   "15": "List/Del collections (replicas > 2)",
                   "E": "----------",
                   "0": "Exit"
                   }


###  TEST ZONE ###

# action_collection_delete_not_active_replicas(URL='http://prana2:8985', collection='my_test_1', shard='shard1')
# exit(555)

##################

##############################################
# MENU block
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
SolrClusters = ClustersConfigs['Clusters']

print(" ### Solr administrate console. ###\n")
SolrCluster = menu_select_cluster(SolrClusters)

if not ClustersConfigs["Clusters"].get(SolrCluster, False):
    print("Cluster not found. Adios...")
    exit(12)

print("Input Solr admin user. ")
solr_Auth = menu_action_data(solr_Auth)

while True:
    userAction, userAction_params = menu_select_action()
    if not userAction:
        print("Action not found or parameters not set...")
        #exit(13)
    elif userAction == "Exit":
        print("Finished. Good bye...")
        exit(0)


    ############################################
    # ACTION block
    print("\n### Execute action:", userAction, " ###")
    SolrURLs = ClustersConfigs["Clusters"].get(SolrCluster, '').get('hosts', [])
    for host in SolrURLs:
        if test_solr(host):
            print("\t### Solr server is online:", host)
            # save logon data
            # solr_Auth_tmp = solr_Auth - make link and solr_Auth brokes
            #solr_Auth_tmp = solr_Auth
            #solr_Auth_tmp["pwd"] = password_encode( solr_Auth_tmp["pwd"] )
            #set_config(solr_Auth_tmp, config_file='solr_last_auth.conf') # not safty
            ##################
            if userAction == "List alias":
                ALIASES = get_aliases(host, userAction_params['pattern'])
                if not ALIASES: print("\tAliases not found.")
                else:
                    print("\t### Aliases:")
                    for al in ALIASES:  print("\t\t", al['name'], "\t", al['collection'])
                    print('')
            elif userAction == "List/Del collections (replicas > 2)":
                get_collections_with_replicas(URL=host, max_replicas = 2, params=userAction_params)
            elif userAction == "Create alias":
                action_alias(action='CREATEALIAS', URL=host, alias=userAction_params['alias_name'], collections=userAction_params['coll_list'])
            elif userAction == "Delete alias":
                action_alias(action='DELETEALIAS', URL=host, alias=userAction_params['alias_name'])
            elif userAction == "Rename alias":
                action_alias_rename(URL=host, from_alias=userAction_params['alias_name'], to_alias=userAction_params['to_alias'])
            elif userAction == "Add collection to alias":
                action_alias_add_collection(URL=host, alias=userAction_params['alias_name'],
                                    collection=userAction_params['coll_name'])
            elif userAction == "Delete collection from alias":
                action_alias_collection_delete(URL=host, collection=userAction_params['coll_name'],
                                               alias=userAction_params['alias_name'])
            elif userAction == "Delete collection":
                action_collection_delete(URL=host, collection=userAction_params['collection'])
            elif userAction == "Delete dead replicas":
                action_collection_delete_not_active_replicas(URL=host,
                                                             collection=userAction_params['collection'],
                                                             shard=userAction_params['shard'],
                                                             host=userAction_params['host'],
                                                             addrelpica=userAction_params['addreplica'],
                                                             delrecoveryrelpica=userAction_params['delrecoveryrelpica'])
            elif userAction == "Add replica":
                action_collection_delete_not_active_replicas(URL=host,
                                                             collection=userAction_params['collection'],
                                                             shard=userAction_params['shard'],
                                                             host=userAction_params['host'],
                                                             addrelpica=True,
                                                             addReplicaIfNotExist=True)

            elif userAction == "Move replica":
                action_collection_move_replicas(URL=host,
                                                collection=userAction_params['collection'],
                                                shard=userAction_params['shard'],
                                                from_host=userAction_params['from_host'],
                                                to_host=userAction_params['to_host'])

                #action_collection_create_replicas(URL=host, collection=userAction_params['collection'],
                #                                             shard=userAction_params['shard'], count=userAction_params['count'])

            elif userAction == "Show host collections":
                    show_host_collections(URL=host,
                                         collection=userAction_params['collection'],
                                         host=userAction_params['host']
                                         )
            else:
                print('Menu  option not found.')


            break # ! need for action on one alive host only
        else:
            print("\t### Solr server is offline:", host)



print("Finished. Good bye...")




