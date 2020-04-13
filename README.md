# use kazoo

## 1.install

```
$ pip3 install kazoo 
```

## 2.need something for db connection

```
$ pip3 install PyMySQL
```

## 3.set config 

```
$vi config.py
```

```
# It's ignore for git
ZHOSTS='1.1.1.1,1.1.1.2'
PORT='2181'
DBHOST='2.2.2.2'
DBUSER='userid'
DBPASSWD='password'
DBNAME='databasename'
DBSQL="replace into solr_shards (`collection`, `shard_name`, `range`, `state`, `replicas`) values (%s, %s, %s, %s, %s)"
DBSQL2="replace into solr_replicas (`shard_name`, `core_node`, `core`, `base_url`, `node_ip`, `state`, `leader`) values (%s, %s, %s, %s, %s, %s, %s)"
DBSQL3="update solr_replicas set `usage`=%s where `core` = %s"
```

## 4.referance
* [kazoodocs](http://kazoo.readthedocs.io/en/latest/basic_usage.html)

