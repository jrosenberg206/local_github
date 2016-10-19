import os
import sys
import warnings
import time
import csv
import json
import gzip
import requests
import MySQLdb
from requests.auth import HTTPDigestAuth
from pprint import pprint
from DbConfig import dbconfig

EXCLUDE_COLS  = ( 'birthday', 'birthmonth', 'birthyear' )
COLUMN_RENAME = { "inventory_transfer_inbound" : {"location" : "inbound_licence",}, 
                  "inventory_transfer" : {"location" : "inbound_licence",}, 
                }
TABLE_RENAME  = { "inventory_transfer_inbound" : "inventory_transfer",
                  "inventory_transfer_outbound" : "inventory_transfer",
                }
TABLE_REUSE   = { "inventory_transfer" : None,
                  "inventory_transfer_outbound" : None,
                }
TABLE_INDEX   = {
                  "biotrackthc_dispensing" : ", INDEX biotrackthc_dispensing_inventoryid (inventoryid), INDEX biotrackthc_dispensing_location (location), INDEX biotrackthc_dispensing_orgid (orgid)",
                  "biotrackthc_inventory" : ", INDEX biotrackthc_inventory_inventorytype (inventorytype)",
                  "biotrackthc_inventorytransfers" : ", INDEX biotrackthc_inventorytransfers_outbound_license (outbound_license), INDEX biotrackthc_inventorytransfers_inventoryid (inventoryid), INDEX biotrackthc_inventorytransfers_inbound_license (inbound_license)",
                  "biotrackthc_inventorytype" : ", INDEX biotrackthc_inventorytype (inventorytype)",
                  "biotrackthc_locations" : ", INDEX biotrackthc_locations_licensenum (licensenum), INDEX biotrackthc_locations_orgid (orgid)",
                }
TABLE_NO_PRIMARY = set( ('biotrackthc_inventorylog',
                         'biotrackthc_plantslog', ) )
COLUMN_TYPE_OVERRIDE = {    # Theses guys can be a comma delimited list of id's
                          ( "inventory", "inventoryparentid" ) : "text",
                          ( "inventory", "parentid" ) : "text",
                          ( "inventory", "plantid" ) : "text",
                          ( "plant", "plantid" ) : "text",
                          ( "biotrackthc_inventory", "inventoryparentid" ) : "text",
                          ( "biotrackthc_inventory", "parentid" ) : "text",
                          ( "biotrackthc_inventory", "plantid" ) : "text",
                          ( "biotrackthc_inventory", "productname" ) : "text",
                          ( "biotrackthc_inventorylog", "inventoryparentid" ) : "text",
                          ( "biotrackthc_inventorytransfers", "parentid" ) : "text",
                          ( "biotrackthc_plant", "plantid" ) : "text",
                       }

def getSchemaFromLicense(license=None, prefix='ubi'):
    if not license:
        license = os.environ['bio_ubi']
    return '{prefix}{license}'.format(prefix=prefix, license=license)

def readCsvFile(fn):
    lines = open(fn).readlines()
    cols = [ col.lower().replace(' ', '_') for col in lines[0].strip().split(',') ]
    rows = [ dict([(cols[i], fields[i]) for i in range(len(cols))]) for fields in [line.strip().split(',') for line in lines[1:]] ]
    return cols, rows
        
def getColumnsAndTypes(rows, table):
    cols = [ col for col in sorted(rows[0]) if col not in EXCLUDE_COLS ]
    col_types = dict( [ (col, getColumnType(col, rows, table)) for col in cols ] )
    return cols, col_types

MIN_INT = -2147483648
MAX_INT = 2147483647
MIN_BIGINT = -9223372036854775808
MAX_BIGINT = 9223372036854775807
def getColumnType(col, rows, table):
    col_type = None
    date_start = 1293840000  # 2011-01-01
    date_end = time.time() + 180 * 24 * 60 * 60  # 6 months in the future
    int_min = 2147483647
    int_max = -2147483648
    max_len = 0
    for row in rows:
        val = row[col]
        if val == '':
            continue
        try:
            i = int(val)
            if col_type == None:
                col_type = 'int'
            int_min = min(int_min, i)
            int_max = max(int_max, i)
        except:
            try:
                f = float(val)
                if col_type != 'tinytext':
                    if 'latitude' in col or 'longitude' in col:
                        col_type = 'decimal(15,7)'
                    else:
                        col_type = 'decimal(15,5)'
            except:
                col_type = 'tinytext'
                if val:
                    max_len = max(max_len, len(val))
    if col_type == 'int' and int_min > date_start and int_max < date_end:
        col_type = 'datetime'
    if col_type == 'int' and (int_min < MIN_INT or int_max > MAX_INT):
        col_type = 'bigint'
    if col_type == 'tinytext' and max_len > 24:   # Prevent dying when some silly description a few months from now goes over 255 bytes
        col_type = 'text'
    col_type = COLUMN_TYPE_OVERRIDE.get((table, col), col_type)
    return col_type

def escapeInsertFields(value, col_type):
    if value == None:
        return 'null'
    if ('int' in col_type or 'decimal' in col_type or 'datetime' == col_type) and value == '':
        return 'null'
    if 'text' in col_type:
        if isinstance(value, unicode):
            value = value.encode('ascii', 'ignore')
        else:
            value = str(value)
        return "'" + MySQLdb.escape_string(value) + "'"
    if 'datetime' == col_type:
        return 'from_unixtime(' + str(value) + ')'
    if value == 'None':
        return 'null'
    if 'decimal' in col_type and '.' in value:
        places = int(col_type.split(',')[1].split(')')[0])
        # print 'places', places, value # MySQLdb.escape_string(str(round(value, places)))  # DEBUG
        return MySQLdb.escape_string(str(round(float(value), places)))
    return MySQLdb.escape_string(str(value))
        
def getInsertStatement(schema, table, cols, col_types, rows): 
    return "INSERT INTO {schema}.{table} ({cols}) VALUES \n  {values}\n;".format(
            schema=schema,
            table=table,
            cols=', '.join( [ COLUMN_RENAME.get(table, {}).get(col, col) for col in cols if col_types[col] ] ),
            values='\n, '.join( [ '(' + ','.join( [ escapeInsertFields(row[col], col_types[col]) for col in cols if col_types[col] ] ) + ')' for row in rows ] ) 
    )

class InvalidCredentialsException(Exception):
    pass
    
class StateCsVLoader:
    def readAndSaveTable(self, dbObj, filename):
        table_name = filename.split('.csv')[0].split('/')[-1]
        if filename.endswith('.gz'):
            f = gzip.open(filename)
        else:
            f = open(filename)
        readCsvStreamAndSaveTable(dbObj, table_name, csv.reader(f))

class StateLoader:
    def __init__(self, boxDomain, boxFolderId):
        self.boxDomain = boxDomain
        self.boxFolderId = boxFolderId
        
    def getStateFileList(self):
        urlBase = 'https://{domain}/s/{folder}'.format(domain=self.boxDomain, folder=self.boxFolderId)
        page = 1
        csvFiles = []
        totalFileCount = 0
        url = urlBase
        while True:
            r = requests.get(url)
            print 'status_code', r.status_code, url
            for line in r.text.splitlines():
                if 'first_load' in line.strip()[:20]:
                    data = line.strip(',').split('=', 1)[1].strip()
                    null = None
                    false = False
                    true = True
                    first_load = eval(data)
                    shared_folder_info = first_load['params']['shared_folder_info']
                    shared_item = str(shared_folder_info['shared_item'])
                    file_count = shared_folder_info['file_count']
                    for dbFile in first_load['db'].values():
                        if dbFile['type'] == 'file':
                            totalFileCount += 1
                            if dbFile['extension'] == 'csv':
                                csvFiles.append( (dbFile['name'], dbFile['typed_id']) )
                    break
            print totalFileCount, file_count
            if totalFileCount >=  file_count:
                break
            page += 1
            url = urlBase + '/{page}/{shared}'.format(page=page, shared=shared_item)
        return csvFiles
                            
    def readAndSaveTable(self, dbObj, name, id):
        table_name = name.split('.csv')[0]
        url = 'https://{domain}/index.php?rm=box_download_shared_file&shared_name={folder}&file_id={id}'.format(domain=self.boxDomain, folder=self.boxFolderId, id=id)
        r = requests.get(url, stream=True)
        readCsvStreamAndSaveTable(dbObj, table_name, csv.reader(r.iter_lines()))


def readCsvStreamAndSaveTable(dbOjb, table_name, lineStream):
        # Grab the first 1000 lines for determining col_types
        priorId = None
        phase = 0
        cnt = 0
        rows = []
        for line in lineStream:
            if phase == 0:
                cols = [ col.lower().replace(' ', '_') for col in line ]
                phase = 1
                continue
            if not line or not line[0]:
                continue
            if len(cols) != len(line):
                print 'len(cols) != len(line): ', len(cols), '!=', len(line)
                print 'cnt =', cnt
                print line
                print
                cnt += 1
                continue
            if table_name == 'biotrackthc_inventory':
                if priorId == line[0]:
                    print 'DUPLICATE ID', line
                    continue
                priorId = line[0]
            cnt += 1
            rows.append( dict([(cols[i], line[i]) for i in range(len(cols))]) )
            if table_name == 'biotrackthc_labresults_samples':
                try:
                    if abs(float(rows[-1]['received_quantity'])) > 2147483647:
                        print 'abs(recieved_quantity) > 2147483647: row =', rows[-1]
                        rows[-1]['received_quantity'] = None
                except:
                    pass
            if len(rows) >= 1000:
                if phase == 1:
                    cols_ignore, col_types = getColumnsAndTypes(rows, table_name)
                    dbObj.dropTable(table_name)
                    dbObj.createTable(table_name, cols, col_types)                
                    phase = 2
                dbObj.insertRows(table_name, cols, col_types, rows, cnt=cnt)
                rows = []
        if rows:
            if phase == 1:
                cols_ignore, col_types = getColumnsAndTypes(rows, table_name)
                print 'col_types', col_types
                dbObj.dropTable(table_name)
                dbObj.createTable(table_name, cols, col_types)                
            dbObj.insertRows(table_name, cols, col_types, rows, cnt=cnt)
        
class ApiLoader:

    def __init__(self, username, password, license_number):
        self.username = username
        self.password = password
        self.license_number = license_number
        self.url = "https://wslcb.mjtraceability.com/serverjson.asp"
    
    def login(self):
        dPost = dict( API='4.0'
                    , action='login'
                    , license_number=self.license_number
                    , username=self.username
                    , password=self.password
                    )
        print '--', dPost            
        res = requests.post(self.url, json=dPost)
        print '--', res.text
        j = res.json()
        if j and j['success']:
            self.sessionid = j['sessionid']
            # print 'sessionid =', sessionid
        else:
            raise InvalidCredentialsException('Failed to get sessionid')
            
    def createSchema(self, dbObj, prefix='ubi'):
        dbObj.createSchema(getSchemaFromLicense(license=self.license_number, prefix=prefix))
        
    def grabAndSaveTables(self, dbObj):

        tables =  ( 'vehicle', 
                    'employee', 
                    'plant_room', 
                    'inventory_room', 
                    'inventory', 
                    'plant', 
                   # 'plant_derivative', 
                    'manifest', 
                    'inventory_transfer_inbound', 
                   # 'inventory_transfer_outbound', 
                    'inventory_transfer', 
                    'sale', 
                    'tax_report', 
                    'vendor', 
                    'qa_lab', 
                    'inventory_adjust', 
                    'inventory_qa_sample',
                  )
   
        for table in tables:
            print
            print 'table:', table
            dPost = dict( API='4.0'
                    , action='sync_' + table
                    , sessionid=self.sessionid
                    )
            res = requests.post(self.url, json=dPost, stream=True)
            itr = res.iter_content(chunk_size=32768)
            first_time = True
            cnt = 0
            for key, rows in grabThousandRows(itr, table):
                if key == 'success':
                    if not rows:
                        print 'Failed Call! (not "success")'
                        break
                elif key == table:
                    if first_time:
                        first_time = False
                        cols, col_types = getColumnsAndTypes(rows, key)
                        key = TABLE_RENAME.get(key, key)
                        if table not in TABLE_REUSE:
                            dbObj.dropTable(key)
                            dbObj.createTable(key, cols, col_types)
                    if len(rows):
                        cnt += len(rows)
                        dbObj.insertRows(key, cols, col_types, rows, cnt=cnt)

def findFirst(chunk, chars, ptr):
    found = [ (chunk.find(char, ptr), char) for char in chars ]
    found = [ item for item in found if item[0] >= 0 ]
    return (None, None) if not found else min(found)
    
def findClosingBraceJson(chunk, ptr):
    ptr_start_looking = ptr
    while True:
        ptr_found = chunk.find('}', ptr_start_looking)
        if ptr_found < 0:
            return ptr, None
        try:
            return ptr_found, json.loads(chunk[ptr:ptr_found+1])
        except:
            ptr_start_looking = ptr_found + 1

def grabThousandRows(itr, table):
    chunk = itr.next()
    assert chunk[0:2] == '{"'
    ptr = 2
    while True:
        # ptr points to a highest-level dictionary key
        if len(chunk) - ptr < 100:
            try:
                chunk = chunk[ptr:] + itr.next()
                ptr = 0
            except StopIteration:
                pass
        ptr_quote = chunk.find('":', ptr)
        assert ptr_quote > ptr
        key = chunk[ptr:ptr_quote]
        ptr_found, char_found = findFirst(chunk, ',[{}', ptr_quote)
        assert ptr_found and char_found
        if ',' == char_found:
            yield key, json.loads(chunk[ptr_quote+2:ptr_found])
            assert chunk[ptr_found+1] == '"'
            ptr = ptr_found+2
            continue
        elif '}' == char_found:
            yield key, json.loads(chunk[ptr_quote+2:ptr_found])
            assert ptr_found + 5 > len(chunk) and "This was supposed to be the last end brace"
            raise StopIteration
        elif '[' == char_found:
            # This is the start of our list of rows. 
            rows = []
            ptr = ptr_found + 1
            assert chunk[ptr] == '{'
            while True:
                ptr, value = findClosingBraceJson(chunk, ptr)
                if value != None:
                    rows.append(value)
                    if len(rows) >= 1000:
                        yield key, rows
                        rows = []
                    if len(chunk) - ptr < 10:
                        try:
                            chunk = chunk[ptr:] + itr.next()
                            ptr = 0
                        except StopIteration:
                            pass
                    if chunk[ptr+1] == ']':
                        ptr += 1
                        if rows:
                            yield key, rows
                        break
                    assert chunk[ptr+1:ptr+3] == ',{'
                    ptr += 2
                    continue
                try:
                    chunk = chunk[ptr:] + itr.next()
                    ptr = 0
                except StopIteration:
                    raise Exception("could not find the match brace to end the row")
        elif '{' == char_found:
            ptr = ptr_found
            while True:
                ptr, value = findClosingBraceJson(chunk, ptr)
                if value != None:
                    yield key, [value]
                    break
                try:
                    chunk = chunk[ptr:] + itr.next()
                    ptr = 0
                except StopIteration:
                    raise Exception("could not find the match brace to end the highest level dictionary value")
        # ptr -> ending ] or } of the highest level dictionary item, expect a comma or a final ending brace
        if len(chunk) - ptr < 104:
            try:
                chunk = chunk[ptr:] + itr.next()
                ptr = 0
            except StopIteration:
                pass
        if chunk[ptr+1] == '}':
            assert ptr + 5 > len(chunk) and "This was supposed to be the last end brace"
            raise StopIteration
        assert chunk[ptr+1:ptr+3] == ',"'
        ptr += 3
        
        
class CsvLoader:

    def __init__(self, filename):
        self.filename = filename
        self.license_number = os.environ['bio_ubi']
    
        
    def readAndSaveTable(self, dbObj):
        table = self.filename[:-4]
        print
        print 'table:', table
        cols, rows = readCsvFile(self.filename)
        col_names, col_types = getColumnsAndTypes(rows, table)
        assert cols == col_names
        dbObj.dropTable(table)
        dbObj.createTable(table, cols, col_types)
        dbObj.insertRows(table, cols, col_types, rows)
                        
def getTetraTrakCredentials():
        db = MySQLdb.connect( dbconfig["TetraTrak"]["dbhost"],
                              dbconfig["TetraTrak"]["dbuser"],
                              dbconfig["TetraTrak"]["dbpassword"],
                              'tetratrakdb',
                              int(dbconfig["TetraTrak"]["dbport"]) )
        cursor = db.cursor(MySQLdb.cursors.DictCursor)
        sql =   """select id, username, password, license_number
                    from tetratrakdb.tbl_reporting_biotrackCrendentials
                    where not isnull(username) and username > ' '
                      and not isnull(password) and password > ' '
                      and not isnull(license_number) and license_number > ' '
                      and businessType = 'Retailer'
                      and isValid = 1
                    order by id;
                """
        rowcnt = cursor.execute(sql)
        return cursor


class DbSaver:

    def __init__(self):
        self.dbuser= dbconfig["LemonHaze"]["dbuser"]
        self.dbpassword = dbconfig["LemonHaze"]["dbpassword"]
        self.dbhost = dbconfig["LemonHaze"]["dbhost"]
        self.dbport = int(dbconfig["LemonHaze"]["dbport"])

    def connect(self):
        print 'connections:', (self.dbhost, self.dbuser, self.dbpassword, 'lemon', self.dbport)
        self.db = MySQLdb.connect(self.dbhost, self.dbuser, self.dbpassword, 'lemon', self.dbport)
        self.db.autocommit(True)
        
    def createSchema(self, schema):
        self.schema = schema
        cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
        cursor._defer_warnings = True
        sql =  'CREATE DATABASE IF NOT EXISTS {schema};'.format(schema=self.schema)
        cursor.execute(sql)
        
    def dropTable(self, table):
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)    
        cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
        cursor._defer_warnings = True
        sql = 'DROP TABLE IF EXISTS {schema}.{table};'.format(schema=self.schema, table=table)
        cursor.execute(sql)
        warnings.filterwarnings('error', category=MySQLdb.Warning)

    def createTable(self, table, cols, col_types):
        cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
        cursor._defer_warnings = True
        sql = "CREATE TABLE IF NOT EXISTS {schema}.{table} ( {cols} {primary} {index} );".format(
            schema=self.schema,
            table=TABLE_RENAME.get(table, table),
            cols=', '.join( [ COLUMN_RENAME.get(table, {}).get(col, col) + ' ' + col_types[col] for col in cols if col_types[col] ] ) ,
            primary=(', PRIMARY KEY (id)' if ('id' in cols and table not in TABLE_NO_PRIMARY) else ''),
            index=TABLE_INDEX.get(table, '')
        )
        print sql
        cursor.execute(sql)

    def insertRows(self, table, cols, col_types, rows, rowRange=None, rowsPerInsert=1000, cnt=0):
        assert rowsPerInsert > 0
        if rowRange:
            rowPtr, rowEnd = rowRange
            rowEnd = min(rowEnd, len(rows))
        else:
            rowPtr, rowEnd = 0, len(rows)

        cursor = self.db.cursor(MySQLdb.cursors.DictCursor)

        for rowPtr in range(rowPtr, rowEnd, rowsPerInsert):
            rowEndLocal = min(rowEnd, rowPtr+rowsPerInsert)
            sql = getInsertStatement( self.schema, table, cols, col_types, rows[rowPtr:rowEndLocal] )
            if len(sql) > 524288:
                # Too Big, Break it into smaller chuncks
                self.insertRows(table, cols, col_types, rows, rowRange=(rowPtr,rowEndLocal), rowsPerInsert=rowsPerInsert//10, cnt=cnt)
            else:
                print 'On Row:', cnt-len(rows)+rowPtr+1, 'thru', cnt-len(rows)+rowEndLocal, 'out of', cnt
                try:
                    cursor.execute(sql)
                except Exception as ex:
                    print sql
                    raise ex
                


TEMP_SKIP_TABLES = """biotrackthc_plantderivatives.csv
                      biotrackthc_growrooms.csv
                      biotrackthc_dispensing.csv
                      biotrackthc_organizations.csv
                      biotrackthc_labresults_potency_analysis.csv
                      biotrackthc_tax_obligation_reports.csv
                      biotrackthc_inventory.csv
                      biotrackthc_inventory_statistics.csv
                      biotrackthc_labresults_samples.csv
                      biotrackthc_inventory_fifteen.csv
                      biotrackthc_plants.csv
                      biotrackthc_inventory_qa_fail_exempt.csv
                      biotrackthc_organizations_labs.csv
                      biotrackthc_plantslog.csv
                      biotrackthc_undo_history.csv
                      biotrackthc_locations.csv
                      biotrackthc_labresults_moisture_content.csv
                      biotrackthc_locations_labs.csv
                      biotrackthc_labresults_solvent_screening.csv
                      biotrackthc_inventoryconversions.csv
                      biotrackthc_labresults_micro_screening.csv
                      biotrackthc_labresults_foreign_matter.csv
                      biotrackthc_inventoryrooms.csv
                      biotrackthc_inventorysamples.csv
                      biotrackthc_inventorycombinations.csv
                      biotrackthc_inventoryadjustments.csv
                      biotrackthc_inventorytransfers.csv""".split()
TEMP_SKIP_TABLES = []


if __name__ == '__main__':
    argc = len(sys.argv)
    if argc == 2 and sys.argv[1].lower() == 'api':
        dbObj = DbSaver()
        dbObj.connect()
        apiObj = ApiLoader(os.environ['bio_username'], 
                           os.environ['bio_password'],
                           os.environ['bio_ubi'])
        apiObj.login()
        apiObj.createSchema(dbObj)
        apiObj.grabAndSaveTables(dbObj)
    if argc >= 3 and ('api', 'all') == tuple(word.lower() for word in sys.argv[1:3]):
        # Override our clever combination code for the new get all the data items
        COLUMN_RENAME = {}
        TABLE_RENAME  = {}
        TABLE_REUSE   = {}        
        dbObj = DbSaver()
        dbObj.connect()
        restart = 0 if argc < 4 else int(sys.argv[3])
        # TODO Loop thru TetraTrak table of credentials
        for cred in getTetraTrakCredentials():
            id = cred['id']
            if id < restart:
                continue
            del cred['id']
            print 'Credentials ID, license:', id, cred['license_number']
            apiObj = ApiLoader(**cred)
            try:
                apiObj.login()
            except InvalidCredentialsException as ex:
                print ex, "Skipping Licensee"
                continue
            apiObj.createSchema(dbObj, prefix='db')
            apiObj.grabAndSaveTables(dbObj)
    elif argc == 2 and sys.argv[1].lower().endswith('.csv'):
        dbObj = DbSaver()
        dbObj.connect()
        dbObj.createSchema(getSchemaFromLicense())
        csvObj = CsvLoader(sys.argv[1])
        csvObj.readAndSaveTable(dbObj)
    elif argc == 2 and sys.argv[1].lower() == 'state':
        dbObj = DbSaver()
        dbObj.connect()
        dbObj.createSchema('state201604')
        stateObj = StateLoader('lcb.app.box.com', '8trte5ubmgsyo16o5ct01rfwzo6e025f')
        # WAS (for 201603) stateObj = StateLoader('lcb.app.box.com', 'pvt8prm130kasca0xctyjken6cjgquwf')
        # WAS (for 2015  ) stateObj = StateLoader('lcb.app.box.com', '7oakb7fuvbvj9y7d53uwembpktsy7tfd')
        for name, id in stateObj.getStateFileList():
            print 'TABLE:', name
            if name in TEMP_SKIP_TABLES:
                continue
            stateObj.readAndSaveTable(dbObj, name, id)
    elif argc == 3 and sys.argv[1].lower() == 'state':
        dbObj = DbSaver()
        dbObj.connect()
        dbObj.createSchema('state')
        stateObj = StateCsVLoader()
        for filename in sys.argv[2].split(','):
            stateObj.readAndSaveTable(dbObj, filename)
    else:
        print '''Usage: LoadEntityDb.py <command>
        <command>  is one of:
            api
            api all
            state
            state <csv_filelist>
            <table>.csv'''

# & ' ( ) + , - . / : _
