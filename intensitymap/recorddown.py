# !/usr/bin/env python3
import json
import subprocess
import os, argparse
import logging
import shutil
import pdb

def download(filename, outdir, content="lmod", force_download=False):
    """"
    Download record
    """
    task_ids = []
    starts = []
    ends = []
    hdmap_versions = []
    with open(filename) as f:
        for line in f:
            splits = line.split(',')
            taskid, s, e = splits[:3]
            task_ids.append(taskid)
            starts.append(s)
            ends.append(e)
            if len(splits) == 4:
                hdmap_versions.append(splits[3])

    for i, task_id in enumerate(task_ids):
        try:
            print(task_id)
            query_cmd = "/mnt/d/code/adb_client_xiaogang/bin/task_client query -t " + task_id
            query = subprocess.Popen(query_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            query.wait(10)
            task_meta = json.loads(query.stdout.read())
            meta = task_meta["data"][0]["meta"]
            start_time = int(meta["start_time"])
            end_time = int(meta["end_time"])
            is_hw60 = int(meta["cmpt_version"].split(".")[0]) > 5
        except KeyboardInterrupt as e:
            raise e
        except Exception as e:
            logging.info("query task meta fails, continue")
            raise e
        logging.info(f"start_time: {start_time} end_time: {end_time} is_hw60: {is_hw60}")
        seq_id = 0
        car_id = task_id.split("_")[0]

        task_time = task_id.split("_")[1]
        beg_time = starts[i]
        stop_time = ends[i]

        if content == "full":
            lidar_topic = (
                "/sensor/at128_fusion/compensator/PointCloud2,"
                + "/sensor/hesai40/Scan,/sensor/hesai90/Scan,/perception/LidarDebug,"
            )
        else:
            lidar_topic = ""

        if content == "lmod" or content == "full":
            topics = (
                "/tf,"
                + "/localization/map_event_region,"
                + "/localization/real_change_result,"
                + lidar_topic
                + "/localization/100hz/localization_pose,"
                + "/detection/lanes,"
                + "/sensor/camera/obstacle/image_left_forward/h265compressed,"
                + "/sensor/camera/obstacle/image_right_forward/h265compressed,"
                + "/sensor/camera/traffic/image_short/h265compressed,"
                + "/pnc/planning "
            )
        elif content == "diff":
            topics = "/tf," + "/localization/map_event_region," + "/localization/real_change_result "
        else:
            raise ValueError(f"Content not exists {content}")

        dump_cmd = (
            "/mnt/d/code/adb_client_xiaogang/bin/task_client dump -t "
            + task_id
            + " --namespace auto_car -s "
            + str(beg_time)
            + " -e "
            + str(stop_time)
            + " --no-param --topics "
            + topics
            + "--output "
            + f'{outdir}/{car_id}_{str(beg_time).split(".")[0]}_{str(stop_time).split(".")[0]}.record'
        )
        logging.info(dump_cmd)
        dump_proc = subprocess.Popen(dump_cmd.split())
        dump_proc.wait()
        
        if content in ['lmod', 'full'] and len(hdmap_versions) != 0:
            map_dir = f'{outdir}/{car_id}_{str(beg_time).split(".")[0]}_{str(stop_time).split(".")[0]}'
            if not os.path.exists(map_dir):
                os.mkdir(map_dir)
            download_hdmap(hdmap_versions[i], map_dir)

def download_hdmap(hdmap_version, map_dir):
    """
    Download hdmap by version
    """
    map_cmd = f"python hdmap-download-tools/get_map.py {hdmap_version} {map_dir}/"
    logging.info(map_cmd)
    map_proc = subprocess.Popen(map_cmd.split())
    map_proc.wait()
    cmd = f"tar -xzf {map_dir}/hdmap/{hdmap_version}.tar.gz -C {map_dir}"
    logging.info(cmd)
    proc = subprocess.Popen(cmd.split())
    proc.wait()
    
    if os.path.exists(f'{map_dir}/map_version_1_4/base_map.bin'):
        shutil.move(f'{map_dir}/map_version_1_4/base_map.bin', map_dir)
        shutil.rmtree(f'{map_dir}/map_version_1_4')
    else:
        shutil.move(f'{map_dir}/map_version_1_4', f'{map_dir}/map')
        map_cmd = f"python hdmap-download-tools/get_map_base.py {hdmap_version} {map_dir}/"
        map_proc = subprocess.Popen(map_cmd.split())
        map_proc.wait()
        cmd = f"tar -xzf {map_dir}/hdmap/{hdmap_version}.tar.gz -C {map_dir}"
        logging.info(cmd)
        proc = subprocess.Popen(cmd.split())
        proc.wait()
        shutil.rmtree(f'{map_dir}/hdmap')
        shutil.move(f'{map_dir}/base/base_map.bin', map_dir)
        shutil.rmtree(f'{map_dir}/base')
        shutil.rmtree(f'{map_dir}/map')
    
if __name__ == "__main__":
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(levelname)s %(asctime)s %(filename)s:%(lineno)d  %(message)s", datefmt="%m/%d/%Y %I:%M:%S"
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(f"download.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--list", default="download_list.txt", help="download list path")
    parser.add_argument("-o", "--output_dir", default="output", help="output path")
    parser.add_argument("-c", "--content", help="full|lmod|diff", default="lmod")

    args = parser.parse_args()
    print(args.list, args.output_dir, args.content)
    # pdb.set_trace()

    download(args.list, args.output_dir, args.content)