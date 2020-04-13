from kazoo.client import KazooClient
import json
import pymysql
import urllib
import re, sys
import config

# truncate tables solr_shards, solr_replicas
def truncDatas(cur):
    try:
        cur.execute("TRUNCATE TABLE solr_shards")
        print("truncate solr_shards done.")
        cur.execute("TRUNCATE TABLE solr_replicas")
        print("truncate solr_replicas done.")
    except pymysql.Error as e:
        print(e)
    return

# Insert Shard info
def saveinfo(cur,params):

    query = DBSQL
    val = (params.get('collection'),params.get('shard'),params.get('range'),params.get('state'),urllib.parse.urlencode(params.get('replica')))
    try:
        cur.execute(query,val)
        print("shard : {}".format(params.get('shard')))
    except pymysql.Error as e:
        print(e)
    return

# Insert Replica info
def saveReplicaInfo(cur,params):

    query = DBSQL2
    try:
        node_ip = (re.findall(r'[0-9]+(?:\.[0-9]+){3}', rparams.get('base_url')))[0]

        isLeader = 0 if rparams.get('leader') == 'false' else 1

        val = (rparams.get('shard'),rparams.get('core_node'),rparams.get('core'),rparams.get('base_url'),node_ip,rparams.get('state'),isLeader)
        #print("val : {}".format(val))

        try:
            cur.execute(query,val)
        except pymysql.Error as e:
            print(e)
            return

    except Exception as e:
        print(e)
        sys.exit(0)

    return

if __name__ == "__main__":

    DBHOST = config.DBHOST
    DBUSER = config.DBUSER
    DBPASSWD = config.DBPASSWD
    DBNAME = config.DBNAME
    DBSQL=config.DBSQL
    DBSQL2=config.DBSQL2

    ZHOSTS = config.ZHOSTS
    
    db = pymysql.connect(DBHOST,DBUSER,DBPASSWD,DBNAME)
    cur = db.cursor()

    try:
        zk = KazooClient(hosts=ZHOSTS,read_only=True)
        zk.start()
    
        # read root
        root = zk.get_children('/')
        print("ROOT(/) : {}".format(root))
    
        # read /collections
        collections = zk.get_children('/collections')
        print("collections(/collections) : {}".format(collections))
    
        # read /collections/tibuzz
        tibuzz = zk.get_children('/collections/tibuzz')
        print("tibuzz(/collections/tibuzz) : {}".format(tibuzz))
    
        # get /collections/tibuzz/state.json
        state_json = zk.get('/collections/tibuzz/state.json')
        #print("state.json(/collections/tibuzz/state.json) : {}".format(state_json))
    
        # change format to json 
        statejson = json.loads(state_json[0])
        #print("statejson : {}".format(statejson))
        #print("statejson keys : {}".format(statejson.keys()))
    
        # get tiuzz
        tibuzz = statejson.get("tibuzz")
        #print("statejson.tibuzz  : {}".format(tibuzz))
        #print("statejson.tibuzz keys  : {}".format(tibuzz.keys()))
    
        # get shards
        shards = tibuzz.get("shards")
        #print("statejson.tibuzz.shards  : {}".format(shards))
        #print("statejson.tibuzz.shards keys  : {}".format(shards.keys()))
   
        # truncate old data solr_shards, solr_replicas
        truncDatas(cur)
        #
        for item in shards.items():
            params = {}
            sname = item[0]
            srange = item[1].get('range','')
            sstate = item[1].get('state','')
            replicas = item[1].get('replicas',{})
            print("replicas : {}".format(replicas))
            params.update({'collection':'tibuzz'})
            params.update({'shard':sname})
            params.update({'range':srange})
            params.update({'state':sstate})
            params.update({'replica':replicas})
            saveinfo(cur,params)

            for replica in replicas.items():
                rparams = {}
                rparams.update({'shard':sname})
                rparams.update({'core_node':replica[0]})
                rparams.update({'core':replica[1].get('core','')})
                rparams.update({'base_url':replica[1].get('base_url','')})
                rparams.update({'state':replica[1].get('state','')})
                rparams.update({'leader':replica[1].get('leader','false')})
                saveReplicaInfo(cur,rparams)

    except Exception as e:
        print("Error : {}".format(e))

    finally:
        zk.stop()
        db.commit()
        db.close()
