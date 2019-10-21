from __future__ import division
from flask import Flask,request,Response,after_this_request
from flask_cors import CORS
import json,base64,cStringIO,gzip,functools,boto,datetime,sys
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import requests as http
from hashlib import md5

app = Flask(__name__)
app.config['SECRET_KEY'] = "S3AppDatabaseWorker"
CORS(app)

global server_host, server_port, true, false, null, conn, bucket

# Configuration
s3bucket_name,s3conn_user,s3conn_pass,s3region,server_host,server_port = sys.argv[1:]

conn = S3Connection(s3conn_user, s3conn_pass, host="s3.%s.amazonaws.com" % s3region)
try:
    bucket = conn.create_bucket(s3bucket_name)
except:
    bucket = conn.get_bucket(s3bucket_name)

true = True; false = False; null = None

def now():
    return str(datetime.datetime.today())

def paginate(array,page_size=None,this_page=None):
    if page_size and this_page:
        array_size = len(array); page_size = int(page_size); this_page = int(this_page) - 1
        try:
            max_pages = len(array)//page_size
            if this_page > max_pages:
                return []
            else:
                return array[this_page*page_size:(this_page+1)*page_size]
        except:
            return []
    else:
        return array

def new_id():
    hasher = md5()
    hasher.update(now())
    return hasher.hexdigest()

def storage_instance_key():
    global bucket
    return Key(bucket)

def store_string_in_s3(keyname,stringdata):
    key = storage_instance_key()
    key.key = keyname
    key.set_contents_from_string(stringdata)
    return None

def fetch_string_from_s3(keyname):
    key = storage_instance_key()
    key.key = keyname
    return key.get_contents_as_string()

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
    # ensure Register is available
    try:
        REGISTER = eval(fetch_string_from_s3("S3AppDatabase.register"))
    except Exception as error:
        # check error message, act contextually
        if "S3ResponseError: 404 Not Found" in str(error):
            # create Register
            store_string_in_s3("S3AppDatabase.register",repr({}))
            # pull again for good measure
            REGISTER = eval(fetch_string_from_s3("S3AppDatabase.register"))
    return REGISTER

def set_register(REGISTER):
    store_string_in_s3("S3AppDatabase.register",repr(REGISTER))
    return null

def get_index(prototype):
    # ensure Index is available
    REGISTER = get_register()
    if prototype in REGISTER:
        try:
            indexfile = "S3AppDatabase.%s.index" % prototype
            INDEX = eval(fetch_string_from_s3(indexfile))
        except Exception as error:
            # check error message, act contextually
            if "S3ResponseError: 404 Not Found" in str(error):
                # create Index
                store_string_in_s3(indexfile,repr({}))
                # pull again for good measure
                INDEX = eval(fetch_string_from_s3(indexfile))
        return INDEX
    else:
        return null

def set_index(prototype,INDEX):
    REGISTER = get_register()
    if prototype in REGISTER:
        indexfile = "S3AppDatabase.%s.index" % prototype
        store_string_in_s3(indexfile,repr(INDEX))
    return null

def get_table(prototype):
    # ensure Table is available
    REGISTER = get_register()
    if prototype in REGISTER:
        try:
            tablefile = "S3AppDatabase.%s.table" % prototype
            TABLE = eval(fetch_string_from_s3(tablefile))
        except Exception as error:
            # check error message, act contextually
            if "S3ResponseError: 404 Not Found" in str(error):
                # create Table
                store_string_in_s3(tablefile,repr({}))
                # pull again for good measure
                TABLE = eval(fetch_string_from_s3(tablefile))
        return TABLE
    else:
        return null

def set_table(prototype,TABLE):
    REGISTER = get_register()
    if prototype in REGISTER:
        tablefile = "S3AppDatabase.%s.table" % prototype
        store_string_in_s3(tablefile,repr(TABLE))
    return null

def update_prototype(prototype,dataform):
    dataform+=[x for x in ["row_id","%s_id" % prototype] if x not in dataform]
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
    REGISTER = get_register()
    def process(array):
        global inner_matches
        inner_matches.extend(array)
        return None
    if prototype in REGISTER:
        INDEX = get_index(prototype)
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
            return fetch_rows(prototype,matches)
        elif mode == "update":
            return update_rows(matches,prototype,value_dict)
        else:
            return matches
    return null

def update_rows(row_ids,prototype,value_dict):
    global REGISTER,INDEX,TABLE
    REGISTER = get_register()
    if prototype in REGISTER:
        INDEX = get_index(prototype)
        TABLE = get_table(prototype)
    else:
        return null
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
            return row_id
        except:
            return null
    result = [update_logical_row(row_id,prototype,value_dict) for row_id in row_ids]
    set_index(prototype,INDEX)
    set_table(prototype,TABLE)
    return result

def update_all_rows(prototype,value_dict):
    try:
        return update_rows(xrange(get_row_count(prototype)+1),prototype,value_dict)
    except:
        return null

def get_row_count(prototype):
    REGISTER = get_register()
    if prototype in REGISTER:
        return REGISTER[prototype]["row_count"]
    else:
        return null

def get_dataform(prototype):
    REGISTER = get_register()
    if prototype in REGISTER:
        return REGISTER[prototype]["dataform"]
    else:
        return null

def fetch_rows(prototype,row_ids):
    REGISTER = get_register()
    if prototype in REGISTER:
        TABLE = get_table(prototype)
        if row_ids == "*":
            return [TABLE[row_id] for row_id in TABLE]
        else:
            return [TABLE[row_id] for row_id in row_ids]
    else:
        return null

def fetch_all_rows(prototype,page_size=None,this_page=None):
    try:
        return paginate(fetch_rows(prototype,"*"),page_size,this_page)
    except:
        return null

def wrap_response(prototype,data):
    return {"data":{"data":{prototype:data}}}

def new_record(prototype,data):
    REGISTER = get_register()
    if prototype in REGISTER:
        dataform = REGISTER[prototype]["dataform"]
        row_count = REGISTER[prototype]["row_count"]
        row_count+=1
        # add row_id and prototype_id if necessary
        prototype_id = "%s_id" % prototype
        if "row_id" not in data:
            data["row_id"] = row_count
        if prototype_id not in data:
            data[prototype_id] = new_id()
        common_fields = [field for field in data if field in dataform]
        # update table
        TABLE = get_table(prototype); TABLE[row_count] = {field:data[field] for field in common_fields}; set_table(prototype,TABLE)
        # retrieve index
        INDEX = get_index(prototype)
        # update index
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
        set_index(prototype,INDEX)
        # update row_count
        REGISTER[prototype]["row_count"] = row_count
        set_register(REGISTER)
        return data[prototype_id]
    else:
        return null

@app.route("/ods/get_schemas")
def get_schemas():
    return responsify(200,"OK",{"schemas":get_register().keys()})

@app.route("/ods/stats")
def get_stats():
    REGISTER = get_register()
    return responsify(200,"OK",{prototype:REGISTER[prototype]["row_count"] for prototype in REGISTER})

@app.route("/ods/new_prototype", methods=["POST"])
def new_prototype():
    formdata = request.get_json(force=True)
    required = ["object","dataform"]
    missing = [key for key in required if key not in formdata.keys()]
    if missing:
        return responsify(404,"error: the following required fields were not found: %s" % str(missing))
    else:
        object = formdata["object"]
        dataform  = formdata["dataform"]
        update_prototype(object,dataform.keys())
        return responsify(201,"prototype updated for object: %s" % object.upper())

@app.route("/ods/get_prototype/<path:tablename>")
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
        prototype_id = new_record(prototype,data)
        if prototype_id:
            constraints = {"%s_id" % prototype:[prototype_id]}
            return responsify(200,"logical table: %s" % prototype,fetch_rows(prototype,search_index(prototype,constraints)))
        else:
            return responsify(400,"Some error occured")
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/fetch_record",methods=["POST"])
@gzipped
def handle_fetch_record():
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
    try:
        formdata = request.get_json(force=True)
        if "page_size" in formdata and "this_page" in formdata:
            page_size = formdata["page_size"]; this_page = formdata["this_page"]
        else:
            page_size = this_page = None
        constraints = formdata["constraints"]
        prototype = formdata["tablename"]
        if constraints == "*":
            return responsify(200,"logical table: %s" % prototype,wrap_response(prototype,fetch_all_rows(prototype,page_size,this_page)))
        else:
            constraints = {key:format_param(constraints[key]) for key in constraints}
            ids = search_index(prototype,constraints,page_size,this_page)
            return responsify(200,"logical table selection: %s %s" % (prototype,ids),wrap_response(prototype,fetch_rows(prototype,ids)))
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/update_record",methods=["POST","PATCH"])
@gzipped
def handle_update_record():
    try:
        formdata = request.get_json(force=True)
        prototype = formdata["tablename"]
        constraints = formdata["constraints"]
        value_dict = formdata["data"]
        ids = search_index(prototype,constraints,"update",value_dict)
        return responsify(200,"Update OK",{"data":fetch_rows(prototype,ids)})
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/search",methods=["POST"])
@gzipped
def handle_search():
    try:
        formdata = request.get_json(force=True)
        constraints = formdata["constraints"]
        prototype = formdata["tablename"]
        if constraints == "*":
            return responsify(200,"logical table: %s" % prototype,fetch_all_rows(prototype))
        else:
            records = search_index(prototype,constraints,"records")
            return responsify(200,"%s search results attached" % len(records),records)
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/reset_row_count/<path:prototype>")
@gzipped
def rrc(prototype):
    try:
        register = get_register()
        if prototype in register:
            register[prototype]["row_count"] = 0
            set_register(register)
        return responsify(200,"Register Updated")
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/get_register")
@gzipped
def gr():
    try:
        return responsify(200,"Register Attached",get_register())
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/set_register",methods=["POST"])
@gzipped
def sr():
    try:
        formdata = request.get_json(force=True)
        return responsify(200,"Register Updated",set_register(formdata["register"]))
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

@app.route("/ods/get_records",methods=["POST"])
@gzipped
def get_records():
    try:
        formdata = request.get_json(force=True)
        prototype = formdata["tablename"]
        row_ids = formdata["row_ids"]
        return responsify(200,"Records Attached",wrap_response(prototype,fetch_rows(prototype,row_ids)))
    except Exception as e:
        return responsify(400,"error clue: %s" % str(e))

if __name__ == "__main__":
    app.run(host=server_host,port=server_port,threaded=true)
