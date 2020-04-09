from kazoo.client import KazooClient
import json
import pymysql
import urllib
import config

def saveinfo(cur,params, sql=''):

    query = DBSQL
    val = (params.get('collection'),params.get('shard'),params.get('range'),params.get('state'),urllib.parse.urlencode(params.get('replica')))
    try:
        cur.execute(query,val)
    except pymysql.Error as e:
        print(e)
    return

if __name__ == "__main__":

    DBHOST = config.DBHOST
    DBUSER = config.DBUSER
    DBPASSWD = config.DBPASSWD
    DBNAME = config.DBNAME
    DBSQL=config.DBSQL

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
        print("state.json(/collections/tibuzz/state.json) : {}".format(state_json))
    
        # change format to json 
        statejson = json.loads(state_json[0])
        print("statejson : {}".format(statejson))
        print("statejson keys : {}".format(statejson.keys()))
    
        # get tiuzz
        tibuzz = statejson.get("tibuzz")
        print("statejson.tibuzz  : {}".format(tibuzz))
        print("statejson.tibuzz keys  : {}".format(tibuzz.keys()))
    
        # get shards
        shards = tibuzz.get("shards")
        print("statejson.tibuzz.shards  : {}".format(shards))
        print("statejson.tibuzz.shards keys  : {}".format(shards.keys()))
    
        #
        for item in shards.items():
            params = {}
            sname = item[0]
            srange = item[1].get('range','')
            sstate = item[1].get('state','')
            replicas = item[1].get('replicas',{})
            print("replicas : {}".format(replicas))
            for replica in replicas.items():
                print(replica[0]) # node_name
                print(replica[1]) # node info
                print(replica[1].get('core')) # node info
                print(replica[1].get('base_url')) # node info
                print(replica[1].get('node_name')) # node info
                print(replica[1].get('state')) # node info
   
                params.update({'collection':'tibuzz'})
                params.update({'shard':sname})
                params.update({'range':srange})
                params.update({'state':sstate})
                params.update({'replica':replicas})

                saveinfo(cur,params)
    
    except:
        print("Error ")
    finally:
        zk.stop()
        db.commit()
        db.close()
