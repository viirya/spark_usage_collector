
import sys
import argparse
import urllib
import json
import time
import pickle

def load_master_file(filename):

    f = open(filename, 'r')
    content = []
    for line in f:
        master_node_info = line.rstrip().split()
        master_node = {"id": master_node_info[0], "url": master_node_info[1]}
        content.append(master_node)

    f.close()

    return content


def fetch_data_from_url(url):

    print("crawling " + url + " ...")
    data = ""
    try:
        data = urllib.urlopen(url).read()
    except:
        print("Exception! skip this url this time.")
        data = ""
    return data

def load_master_info(master_url):

    json_url = master_url + "/json/"
    json = fetch_data_from_url(json_url)
    return json

def parse_master_json(json_str):
    info_obj = json.loads(json_str)
    return info_obj

def main():
    parser = argparse.ArgumentParser(description = 'Collect and parse usage info for Spark cluster.')
    parser.add_argument('-f', help = 'The file containing the list of Spark master nodes.')
    parser.add_argument('-s', help = 'The time interval in seconds to sleep between each querying master nodes.')
    parser.add_argument('-o', help = 'The output pickle file prefix.')
    parser.add_argument('-n', help = 'How many times this script runs.')

    args = parser.parse_args()

    master_list = load_master_file(args.f)
    running_times = 0
    while running_times < int(args.n):
        masters = []
        cur_time = time.localtime()
        query_time = time.strftime("%Y-%m-%d %H:%M:%S", cur_time)

        parsed_master_nodes = {}

        for master_node in master_list:
            master_id = master_node["id"]
            master_url = master_node["url"]

            json_str = load_master_info(master_url)
            if json_str == "":
                continue

            master_info = parse_master_json(json_str)
        
            cores = master_info["cores"]
            cores_used = master_info["coresused"]
            memory = master_info["memory"]
            memory_used = master_info["memoryused"]
            activeapps = master_info["activeapps"]

            master = {}
            if not master_id in parsed_master_nodes:
                master["master_url"] = master_url
                master["query_time"] = query_time
                master["cores"] = cores
                master["cores_used"] = cores_used
                master["memory"] = memory
                master["memory_used"] = memory_used
                master["activeapps"] = activeapps

                parsed_master_nodes[master_id] = master
            else:
                master = parsed_master_nodes[master_id]

                master["master_url"] += " && " + master_url
                master["cores"] = max(cores, master["cores"])
                master["cores_used"] = max(cores_used, master["cores"])
                master["memory"] = max(memory, master["memory"])
                master["memory_used"] = max(memory_used, master["memory_used"])
                master["activeapps"] = master["activeapps"] + activeapps

                parsed_master_nodes[master_id] = master
        
            print(master)

        for master_id in parsed_master_nodes.keys():
            masters.append(parsed_master_nodes[master_id])

        output = open(args.o + "-" + time.strftime("%Y-%m-%d-%H-%M-%S", cur_time) + ".pkl", 'wb')
        pickle.dump(masters, output)
        output.close()

        time.sleep(int(args.s))
        running_times += 1


if __name__ == "__main__":
    main()

