# DBSetup.py
import sys, subprocess,os,datetime
import requests as http

THIS_DIR = os.getcwd()
if "\\" in THIS_DIR:
    slash = "\\"; sudo = ""
else:
    slash = "/"; sudo = "sudo "
THIS_DIR+=slash

config_file = THIS_DIR+"S3AppDatabase.config"
gateway_file_url = "https://raw.githubusercontent.com/cmdimkpa/S3AppDatabaseWorker/master/DBGateway.js"

try:
    mode = sys.argv[1]
except:
    print("No mode selected. Exiting...")
    sys.exit()

def now():
    return datetime.datetime.today()

def elapsed(t):
    return "took: %s secs" % (now() - t).seconds

def report(task,breakpoint):
    print("Completed task: %s, %s" % (task,elapsed(breakpoint)))

def read_config():
    p = open(config_file,"rb+")
    config = p.readlines()
    p.close()
    return {line.split("\r\n")[0].split(":")[0]:line.split("\r\n")[0].split(":")[1] for line in config}

def write_config(config):
    p = open(config_file,"wb+")
    lines = ["%s:%s\r\n" % (key,config[key]) for key in config]
    p.writelines(lines)
    p.close()
    return config

def make_gateway_file(src):
    gateway_file = THIS_DIR+"DBGateway.js"; p = open(gateway_file,"wb+"); p.write(src); p.close()
    package_file = THIS_DIR+"package.json"; p = open(package_file,"wb+"); p.write("{}"); p.close()
    return gateway_file

def run_shell(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = p.communicate()
    if err:
        return err
    else:
        try:
            return eval(out)
        except:
            return out

if mode == "edit_config":
    try:
        config = read_config()
    except:
        config = {
            "s3bucket_name":None,
            "s3conn_user":None,
            "s3conn_pass":None,
            "s3region":None,
            "server_host":None,
            "server_port":None
        }
    for key in config:
        print("%s: ? %s" % (key,config[key]))
        entry = raw_input()
        if entry:
            config[key] = entry
    print(write_config(config))
elif mode == "show_config":
    try:
        print(read_config())
    except:
        print(write_config({}))
elif mode == "build_config":
    try:
        BUILD_STAGES = 4
        BUILD_STAGE = 1
        BUILD_STAGE_DESCR = "Create Gateway Node Environment"
        BUILD_TASK = "Download gateway file"; breakpoint = now()
        gateway_src = http.get(gateway_file_url).content
        report(BUILD_TASK,breakpoint)
        BUILD_TASK = "Read Config"; breakpoint = now()
        Config = read_config()
        report(BUILD_TASK,breakpoint)
        BUILD_TASK = "Update environment variables"; breakpoint = now()
        gateway_src = gateway_src.replace("__DB_GATEWAY_PORT__",Config["server_port"])
        gateway_src = gateway_src.replace("__DB_SERVER_HOST__",Config["server_host"])
        gateway_src = gateway_src.replace("__DB_SERVER_PORT__",str(int(Config["server_port"])+1))
        gateway_file = make_gateway_file(gateway_src)
        print("gateway file: %s" % gateway_file)
        report(BUILD_TASK,breakpoint)
        BUILD_TASK = "Set Install Path"; breakpoint = now()
        run_shell("cd %s" % THIS_DIR)
        report(BUILD_TASK,breakpoint)
        BUILD_TASK = "Require Node Modules"; breakpoint = now()
        print(run_shell("%snpm install --save express body-parser request cors compression" % sudo))
        print(run_shell("%snpm install -g forever" % sudo))
        report(BUILD_TASK,breakpoint)
        BUILD_TASK = "Start Gateway Service"; breakpoint = now()
        print(run_shell("%s forever start -c node %s" % (sudo,"DBGateway.js")))
        report(BUILD_TASK,breakpoint)
    except Exception as error:
        print("BuildError: [Build Stage: %s/%s, Build Process: %s -> %s] : %s" % (BUILD_STAGE,BUILD_STAGES,BUILD_STAGE_DESCR,BUILD_TASK,str(error)))
else:
    print("mode: [%s] unknown, exiting..." % mode)
    sys.exit()
