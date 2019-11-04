from __future__ import division
from flask import Flask,request,Response,after_this_request
from flask_cors import CORS
import json,base64,cStringIO,gzip,functools,boto,datetime,sys,time
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import requests as http
from hashlib import md5
from threading import Thread

app = Flask(__name__)
app.config['SECRET_KEY'] = "S3AppDatabaseWorker"
CORS(app)

global server_host, server_port, true, false, null, conn, bucket, MESSAGE_BUS

s3bucket_name,s3conn_user,s3conn_pass,s3region,server_host,server_port = sys.argv[1:]
conn = S3Connection(s3conn_user, s3conn_pass, host="s3.%s.amazonaws.com" % s3region)
try:
    bucket = conn.create_bucket(s3bucket_name)
except:
    bucket = conn.get_bucket(s3bucket_name)
true = True; false = False; null = None; MESSAGE_BUS = {}

def now():
    return str(datetime.datetime.today())

def timestamp():
    return int(time.time())

def paginate(array,page_size,this_page):
    array_size = len(array)
    if "-" not in str(this_page):
        skip = page_size*(this_page-1); next = page_size*this_page
        if skip < array_size:
            try:
                return array[skip:next]
            except:
                return array[skip:]
        else:
            return []
    else:
        skip = array_size - abs(this_page)*page_size; next = array_size - (abs(this_page)-1)*page_size
        try:
            return array[skip:next]
        except:
            return []

def new_id():
    hasher = md5()
    hasher.update(now())
    return hasher.hexdigest()

def network_event_handler(event,slot_key):
    global MESSAGE_BUS
    try:
        def delete_data(params):
            keyname = params[0]
            key = Key(bucket); key.key = keyname; key.delete()
            return null
        def write_data(params):
            keyname,stringdata = params
            key = Key(bucket); key.key = keyname; key.set_contents_from_string(stringdata)
            return null
        def read_data(params):
            keyname = params[0]
            key = Key(bucket); key.key = keyname
            try:
                content = eval(key.get_contents_as_string())
            except:
                content = {}
            return {keyname:content}
    except:
        return null
    params = [event["keyname"]]
    if "write_data" in event["event"]:
        params.append(event["datastring"])
    MESSAGE_BUS[slot_key].append(eval(event["event"])(params))
    return null

def RunParallelS3Events(Events,slot_key):
    global MESSAGE_BUS
    MESSAGE_BUS[slot_key] = []; workers = []
    for event in Events:
        worker = Thread(target=network_event_handler, args=(event,slot_key))
        #worker.daemon = true
        worker.start(); workers.append(worker)
    for worker in workers:
        worker.join()

def AsyncS3MessagePolling(Events):
    global MESSAGE_BUS
    slot_key = new_id(); RunParallelS3Events(Events,slot_key)
    Message = MESSAGE_BUS[slot_key]; del MESSAGE_BUS[slot_key]
    result = [data for data in Message if data]
    if result:
        message = {entry.keys()[0]:entry[entry.keys()[0]] for entry in result}
    else:
        message = null
    return message

def responsify(status,message,data={}):
    code = int(status)
    a_dict = {"data":data,"message":message,"code":code}
    try:
        return Response(json.dumps(a_dict), status=code, mimetype='application/json')
    except:
        return Response(str(a_dict), status=code, mimetype='application/json')

def gzipped(f):
    @functools.wraps(f)
    def view_func(*args, **kwargs):
        @after_this_request
        def zipper(response):
            accept_encoding = request.headers.get('Accept-Encoding', '')
            if 'gzip' not in accept_encoding.lower():
                return response
            response.direct_passthrough = False
            if (response.status_code < 200 or
                response.status_code >= 300 or
                'Content-Encoding' in response.headers):
                return response
            gzip_buffer = cStringIO.StringIO()
            gzip_file = gzip.GzipFile(mode='wb',fileobj=gzip_buffer)
            gzip_file.write(response.data)
            gzip_file.close()
            response.data = gzip_buffer.getvalue()
            response.headers['Content-Encoding'] = 'gzip'
            response.headers['Vary'] = 'Accept-Encoding'
            response.headers['Content-Length'] = len(response.data)
            return response
        return f(*args, **kwargs)
    return view_func

def get_register():
    Message = AsyncS3MessagePolling([
        {"event":"read_data","keyname":"S3AppDatabase.register"},
    ])
    REGISTER = Message["S3AppDatabase.register"]
    if not REGISTER:
        AsyncS3MessagePolling([
            {"event":"write_data","keyname":"S3AppDatabase.register","datastring":repr({})},
        ])
        REGISTER = {}
    return REGISTER

def set_register(REGISTER):
    AsyncS3MessagePolling([
        {"event":"write_data","keyname":"S3AppDatabase.register","datastring":repr(REGISTER)},
    ])
    return null

def get_table(prototype):
    tablename = str("S3AppDatabase.%s.table" % prototype)
    Message = AsyncS3MessagePolling([
        {"event":"read_data","keyname":"S3AppDatabase.register"},
        {"event":"read_data","keyname":tablename},
    ])
    REGISTER = Message["S3AppDatabase.register"]
    TABLE = Message[tablename]
    if not REGISTER:
        AsyncS3MessagePolling([
            {"event":"write_data","keyname":"S3AppDatabase.register", "datastring":repr({})},
        ])
    return TABLE,REGISTER

def get_table_and_index(prototype):
    tablename = str("S3AppDatabase.%s.table" % prototype)
    indexname = str("S3AppDatabase.%s.index" % prototype)
    Message = AsyncS3MessagePolling([
        {"event":"read_data","keyname":"S3AppDatabase.register"},
        {"event":"read_data","keyname":tablename},
        {"event":"read_data","keyname":indexname},
    ])
    REGISTER = Message["S3AppDatabase.register"]
    TABLE = Message[tablename]
    INDEX = Message[indexname]
    if not REGISTER:
        AsyncS3MessagePolling([
            {"event":"write_data","keyname":"S3AppDatabase.register","datastring":repr({})}
        ])
    return TABLE,INDEX,REGISTER

def set_table_and_index(prototype,TABLE,INDEX,REGISTER):
    if prototype in REGISTER:
        tablename = str("S3AppDatabase.%s.table" % prototype)
        indexname = str("S3AppDatabase.%s.index" % prototype)
        AsyncS3MessagePolling([
            {"event":"write_data","keyname":tablename,"datastring":repr(TABLE)},
            {"event":"write_data","keyname":indexname,"datastring":repr(INDEX)},
            {"event":"write_data","keyname":"S3AppDatabase.register","datastring":repr(REGISTER)},
        ])
    return null

def update_prototype(prototype,dataform):
    dataform+=[x for x in ["__created_at__","__updated_at__","__private__","row_id","%s_id" % prototype] if x not in dataform]
    REGISTER = get_register()
    if prototype in REGISTER:
        REGISTER[prototype]["dataform"]+=[x for x in dataform if x not in REGISTER[prototype]["dataform"]]
    else:
        REGISTER[prototype] = {"dataform":dataform,"row_count":0}
    set_register(REGISTER)
    return null

def datatype(element):
    type_ = str(type(element))
    if "str" in type_ or "unicode" in type_:
        return 0
    elif "int" in type_ or "float" in type_:
        return 1
    else:
        return 2

def is_partial_match(string,options):
    string = string.lower()
    def is_date(string):
        if "/" in string:
            delim = "/"
        elif "-" in string:
            delim = "-"
        else:
            return false
        if len([x for x in string if x == delim]) == 2 and len(string)>=10:
            comps = string[:10].split(delim)
            try:
                return map(lambda x:str(type(eval(x))),comps) == ["<type 'int'>", "<type 'int'>", "<type 'int'>"]
            except:
                return false
        else:
            return false
    if not is_date(string):
        for option in options:
            if option in string:
                return true
        return false
    else:
        return is_range_match(string,options)

def is_range_match(number,vector):
    return number>=vector[0] and number<=vector[1]

def search_index(prototype,constraints,mode="rows",value_dict={},page_size=None,this_page=None):
    global inner_matches
    def process(array):
        global inner_matches
        inner_matches.extend(array)
        return None
    try:
        TABLE,INDEX,REGISTER = get_table_and_index(prototype)
        dataform = REGISTER[prototype]["dataform"]
        common_fields = [field for field in constraints if field in dataform and field in INDEX]
        matches = []
        for field in common_fields:
            inner_matches = []
            keys = INDEX[field].keys(); boundary = constraints[field]
            [process(INDEX[field][key]) for key in keys if bool(datatype(key) == 0 and is_partial_match(key,boundary)) or bool(datatype(key) == 1 and is_range_match(key,boundary))]
            matches.append(inner_matches)
        intersect = set(matches[0])
        for match in matches[1:]:
            intersect = intersect.intersection(set(match))
        matches = paginate(list(intersect),page_size,this_page)
        if mode == "records":
            return [TABLE[row_id] for row_id in matches]
        elif mode == "update":
            return update_rows(INDEX,TABLE,REGISTER,matches,prototype,value_dict)
        else:
            return matches
    except:
        return null

def update_rows(index,table,register,row_ids,prototype,value_dict):
    global INDEX,TABLE,REGISTER
    INDEX=index; TABLE=table; REGISTER=register
    try:
        def update_logical_row(row_id,prototype,value_dict):
            global INDEX,TABLE
            try:
                dataform = REGISTER[prototype]["dataform"]
                common_fields = [field for field in value_dict if field in dataform]
                for field in common_fields:
                    TABLE[row_id][field] = value_dict[field]
                    try:
                        for value in INDEX[field]:
                            if row_id in INDEX[field][value]:
                                INDEX[field][value].remove(row_id)
                    except:
                        pass
                    new_value = value_dict[field]
                    typestr = str(type(new_value))
                    if "dict" in typestr or "list" in typestr:
                        new_value = repr(new_value)
                    if field in INDEX:
                        if new_value in INDEX[field]:
                            INDEX[field][new_value].append(row_id)
                        else:
                            INDEX[field][new_value] = [row_id]
                    else:
                        INDEX[field] = {new_value:[row_id]}
                TABLE[row_id]["__updated_at__"] = timestamp()
                return row_id
            except:
                return null
        result = [x for x in [update_logical_row(row_id,prototype,value_dict) for row_id in row_ids] if x]
        set_table_and_index(prototype,TABLE,INDEX,REGISTER)
        return [TABLE[row_id] for row_id in result]
    except:
        return null

def get_dataform(prototype):
    REGISTER = get_register()
    if prototype in REGISTER:
        return REGISTER[prototype]["dataform"]
    else:
        return null

def fetch_rows(prototype,row_ids):
    try:
        TABLE,REGISTER = get_table(prototype)
        if prototype in REGISTER:
            if row_ids in ["*",{}]:
                return [TABLE[row_id] for row_id in TABLE]
            else:
                return [TABLE[row_id] for row_id in row_ids]
        else:
            return null
    except:
        return null

def new_record(prototype,data):
    try:
        TABLE,INDEX,REGISTER = get_table_and_index(prototype)
        dataform = REGISTER[prototype]["dataform"]
        row_count = REGISTER[prototype]["row_count"]
        row_count+=1
        data["__created_at__"] = timestamp()
        data["__updated_at__"] = null
        data["__private__"] = 0
        prototype_id = "%s_id" % prototype
        if "row_id" not in data:
            data["row_id"] = row_count
        if prototype_id not in data:
            data[prototype_id] = new_id()
        common_fields = [field for field in data if field in dataform]
        TABLE[row_count] = {field:data[field] for field in common_fields}
        for field in common_fields:
            value = data[field]
            typestr = str(type(value))
            if "dict" in typestr or "list" in typestr:
                value = repr(value)
            if field in INDEX:
                if value in INDEX[field]:
                    INDEX[field][value].append(row_count)
                else:
                    INDEX[field][value] = [row_count]
            else:
                INDEX[field] = {value:[row_count]}
        REGISTER[prototype]["row_count"] = row_count
        set_table_and_index(prototype,TABLE,INDEX,REGISTER)
        return data
    except:
        return null

def format_param(param):
    def normalize(i):
        type_ = str(type(i))
        if "str" in type_ or "unicode" in type_:
            return str(i).lower()
        else:
            return i
    type_ = str(type(param))
    if "list" in type_:
        return map(normalize,param)
    else:
        return [normalize(param)]*2

@app.route("/ods/get_schemas")
def get_schemas():
    return responsify(200,"OK",{"schemas":get_register().keys()})

@app.route("/ods/stats")
def get_stats():
    REGISTER = get_register()
    return responsify(200,"OK",{prototype:REGISTER[prototype]["row_count"] for prototype in REGISTER})

@app.route("/ods/new_table", methods=["POST"])
def new_prototype():
    formdata = request.get_json(force=True)
    required = ["tablename","fields"]
    missing = [key for key in required if key not in formdata.keys()]
    if missing:
        return responsify(404,"error: the following required fields were not found: %s" % str(missing))
    else:
        object = formdata["tablename"]
        dataform  = formdata["fields"]
        update_prototype(object,dataform)
        return responsify(201,"prototype updated for object: %s" % object.upper())

@app.route("/ods/get_fields/<path:tablename>")
def get_prototype(tablename):
    prototype = get_dataform(tablename)
    if prototype:
        return responsify(200,"OK",prototype)
    else:
        return responsify(404,"error: prototype not found for: %s" % tablename.upper())

@app.route("/ods/new_record",methods=["POST"])
@gzipped
def handle_new_record():
    try:
        formdata = request.get_json(force=True)
        prototype = formdata["tablename"]
        data = formdata["data"]
        return responsify(200,"new record on: %s" % prototype,new_record(prototype,data))
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/fetch_records",methods=["POST"])
@gzipped
def handle_fetch_records():
    try:
        formdata = request.get_json(force=True)
        if "page_size" in formdata and "this_page" in formdata:
            page_size = formdata["page_size"]; this_page = formdata["this_page"]
        else:
            page_size = this_page = None
        constraints = formdata["constraints"]
        prototype = formdata["tablename"]
        if constraints in ["*",{}]:
            constraints = {"__private__":0}
        else:
            constraints["__private__"] = 0
        constraints = {key:format_param(constraints[key]) for key in constraints}
        return responsify(200,"logical table selection: %s" % prototype,search_index(prototype,constraints,"records",{},page_size,this_page))
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/update_records",methods=["POST"])
@gzipped
def handle_update_records():
    try:
        formdata = request.get_json(force=True)
        constraints = formdata["constraints"]
        prototype = formdata["tablename"]
        value_dict = formdata["data"]
        constraints["__private__"] = 0
        constraints = {key:format_param(constraints[key]) for key in constraints}
        return responsify(200,"updated table selection: %s" % prototype,search_index(prototype,constraints,"update",value_dict))
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/delete_records",methods=["POST"])
@gzipped
def handle_delete_records():
    try:
        formdata = request.get_json(force=True)
        constraints = formdata["constraints"]
        prototype = formdata["tablename"]
        value_dict = {"__private__":1}
        constraints = {key:format_param(constraints[key]) for key in constraints}
        return responsify(200,"deleted table selection: %s" % prototype,search_index(prototype,constraints,"update",value_dict))
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/flush_table/<path:prototype>")
@gzipped
def rrc(prototype):
    try:
        register = get_register()
        if prototype in register:
            register[prototype]["row_count"] = 0
            AsyncS3MessagePolling([
                {"event":"write_data","keyname":"S3AppDatabase.register","datastring":repr(register)},
                {"event":"delete_data","keyname":str("S3AppDatabase.%s.index" % prototype)},
                {"event":"delete_data","keyname":str("S3AppDatabase.%s.table" % prototype)},
            ])
            return responsify(200,"Table: [%s] flushed" % prototype)
        else:
            return responsify(400,"No such table: %s" % prototype)
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/get_register")
@gzipped
def gr():
    try:
        return responsify(200,"Register Attached",get_register())
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/get_rows",methods=["POST"])
@gzipped
def get_records():
    try:
        formdata = request.get_json(force=True)
        prototype = formdata["tablename"]
        row_ids = formdata["row_ids"]
        return responsify(200,"Records Attached",fetch_rows(prototype,row_ids))
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/db_test/<path:type>")
@gzipped
def db_test(type):
    type = type.lower()
    def test_post():
        return new_record("Cars",{
            "make":"Acura",
            "model":"Legend",
            "year":1998
        })
    try:
        if type not in ["post","fetch","update"]:
            return responsify(400,"load type: %s not supported" % type.upper())
        else:
            # ensure `Cars` test model is available
            update_prototype("Cars",["make","model","year"]); test_post()
            # select and run test
            if type == "post":
                test_result = test_post()
            if type == "fetch":
                test_result = search_index("Cars",{"__private__":[0,0]},"records",{},1000,1)
            if type == "update":
                test_result = search_index("Cars",{"__private__":[0,0]},"update",{"year":2000},1000,1)
        return responsify(200,"Test Successful",test_result)
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

if __name__ == "__main__":
    app.run(host=server_host,port=server_port,threaded=true)
