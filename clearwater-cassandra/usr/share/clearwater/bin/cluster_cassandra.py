#!/usr/bin/python

import os
import sys
import socket
import time
import etcd
import yaml

# 1st argument should be the address of our etcd discovery service.
etcd_ip = sys.argv[1]

# 2nd argument should be the private IP address to use as our new Cassandra
# seed.
cassandra_ip = sys.argv[2]

# Some useful constants.
seeds_location = "/clearwater/homestead/clustering/"
local_seed_location = seeds_location + etcd_ip + "/seed"
cassandra_yaml_file = "/etc/cassandra/cassandra.yaml"

# Create the etcd client.
client = etcd.Client(host=etcd_ip, port=4001)

# Find the existing seeds, and build up a comma separated list of them. We
# only want to include ourselves in the list if the list was otherwise empty -
# i.e. we're the only cassandra node in the cluster.
r = client.read(seeds_location, recursive=True)
seeds_list = []

for child in r.children:
  if child != cassandra_ip:
    seeds_list.append(child.value)

if len(seeds_list) == 0:
  seeds_list_str = cassandra_ip
else:
  seeds_list_str = ','.join(map(str, seeds_list))

# Read cassandra.yaml.
with open(cassandra_yaml_file) as f:
  doc = yaml.load(f)

# Fill in the correct listen_address and seeds values in the yaml document.
doc["listen_address"] = cassandra_ip
doc["seed_provider"][0]["parameters"][0]["seeds"] = seeds_list_str

# Write back to cassandra.yaml.
with open(cassandra_yaml_file, "w") as f:
  yaml.dump(doc, f)

# Restart Cassandra and make sure it picks up the new list of seeds.
os.system("monit unmonitor cassandra")
os.system("service cassandra stop")
os.system("rm -rf /var/lib/cassandra/")
os.system("mkdir -m 755 /var/lib/cassandra")
os.system("chown -R cassandra /var/lib/cassandra")
os.system("service cassandra start")

# Once we've connected, add the new seed to etcd. First wait until we can
# connect on port 9160 - i.e. Cassandra is running.
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
connect = False

while not connect:
  try:
    s.connect(("localhost", 9160))
    connect = True
    break
  except:
    time.sleep(1)

# Monitor Cassandra again and add the new seed.
os.system("monit monitor cassandra")
client.write(local_seed_location, cassandra_ip)
