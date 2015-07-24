
import sys
import argparse
import os
import pickle

def get_files_in_dir(dirname, postfix = ".pkl"):

    files = []
    fullpath_files = []
    filelist = os.listdir(dirname)
    for filename in filelist:
        if filename.endswith(postfix):
            full_filename = dirname + '/' + filename
            files.append(filename)
            fullpath_files.append(full_filename)

    return (files, fullpath_files)

def parse_pickle(pickleFile):
    pkl_file = open(pickleFile, 'rb')
    masters = pickle.load(pkl_file)
    pkl_file.close()
    return masters

def main():
    parser = argparse.ArgumentParser(description = 'Parse Spark cluster usage pickle files.')
    parser.add_argument('-d', help = 'The directory of pickle files.')
    parser.add_argument('-o', help = 'The output file.')

    args = parser.parse_args()

    masters_info = {}
    usage_data = {}
    usage_cpu = {"used": {}, "unused": {}}
    usage_memory = {"used": {}, "unused": {}}

    (files, fullpaths) = get_files_in_dir(args.d)
    for afile in fullpaths:
        masters = parse_pickle(afile)

        for master in masters:
            if not master["master_url"] in masters_info:
                masters_info[master["master_url"]] = []
                usage_data[master["master_url"]] = []
                usage_cpu["used"][master["master_url"]] = []
                usage_cpu["unused"][master["master_url"]] = []
                usage_memory["used"][master["master_url"]] = []
                usage_memory["unused"][master["master_url"]] = []
 

            masters_info[master["master_url"]].append(master)

    for master_url in masters_info.keys():
        print("master_url: " + master_url)

        historical_master_data = masters_info[master_url]
        sorted_data = sorted(historical_master_data, key = lambda master: master["query_time"])
        if len(sorted_data) > 0:
            header = ["query_time", "cores", "cores_used", "memory", "memory_used", "activeapps"]
            print("\t".join(header))
            for master_data in sorted_data:
                line = []
                for field in header:
                    if field != "activeapps":
                        line.append(str(master_data[field]))
                    else:
                        line.append(str(len(master_data[field])))
                        # we only calculate usage ration when the cluster has working slaves
                        if master_data["cores"] > 0 and master_data["memory"] > 0:
                            if len(master_data[field]) > 0:
                                usage_data[master_url].append(1)
                                usage_cpu["used"][master_url].append(master_data["cores_used"])
                                usage_memory["used"][master_url].append(master_data["memory_used"])
                            else:
                                usage_data[master_url].append(0)
                                usage_cpu["unused"][master_url].append(master_data["cores"])
                                usage_memory["unused"][master_url].append(master_data["memory"])
 

                print("\t".join(line))

    ratios = [0.0]
    cpu_ratios = [0.0]
    memory_ratios = [0.0]

    total_used_cpu = 0.0
    total_unused_cpu = 0.0
    total_used_memory = 0.0
    total_unused_memory = 0.0

    for master_url in masters_info.keys():
        print("master_url: " + master_url)
        ratio = 0.0
        cpu_ratio = 0.0
        memory_ratio = 0.0
        if len(usage_data[master_url]) > 0:
            ratio = sum(usage_data[master_url]) / float(len(usage_data[master_url]))
            ratios.append(ratio)
            cpu_ratio = sum(usage_cpu["used"][master_url]) / float((sum(usage_cpu["used"][master_url]) + sum(usage_cpu["unused"][master_url])))
            cpu_ratios.append(cpu_ratio)
            memory_ratio = sum(usage_memory["used"][master_url]) / float((sum(usage_memory["used"][master_url]) + sum(usage_memory["unused"][master_url])))
            memory_ratios.append(memory_ratio)

            total_used_cpu += sum(usage_cpu["used"][master_url])
            total_unused_cpu += sum(usage_cpu["unused"][master_url])
            total_used_memory += sum(usage_memory["used"][master_url])
            total_unused_memory += sum(usage_memory["unused"][master_url])
 
        print("cluster usage ratio: " + str(ratio))
        print("cluster cpu usage ratio: " + str(cpu_ratio))
        print("cluster memory usage ratio: " + str(memory_ratio))

    print("avg usage ratio: " + str(sum(ratios) / float(len(ratios))))
    print("avg cpu usage ratio: " + str(sum(cpu_ratios) / float(len(cpu_ratios))))
    print("avg memory usage ratio: " + str(sum(memory_ratios) / float(len(memory_ratios))))
 
    print("total cpu usage ratio: " + str(total_used_cpu / float(total_used_cpu + total_unused_cpu)))
    print("total memory usage ratio: " + str(total_used_memory / float(total_used_memory + total_unused_memory)))
        

if __name__ == "__main__":
    main()


