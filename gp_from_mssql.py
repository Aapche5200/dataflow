import pymssql
import re

business = "oa"
dbname = "ABV5"


conn = pymssql.connect(host='10.50.67.14',
                       port='1433',
                       user='gqreader',
                       password='G!fxRYrcxw$usVza',
                       database=dbname
                       )


"""conn = pymssql.connect(host='10.20.121.228',
                       port='1433',
                       user='gpreader',
                       password='Y0qV#$xF!7aOvJso',
                       database=dbname
                       )
                       """

cursor = conn.cursor()
table_list = [
"formson_1120"
]

for table in table_list:
    head = "DROP EXTERNAL TABLE IF EXISTS ex_ods_%s_%s;\n" \
           "CREATE READABLE EXTERNAL TABLE ex_ods_%s_%s (\n" % (business + "_" + dbname, table, business + "_" + dbname, table)
    end = "LOCATION('pxf://dbo.%s?PROFILE=jdbc&SERVER=test-%s') \n\
    FORMAT 'CUSTOM' (formatter='pxfwritable_import');" % (table, business + "-" + dbname)
    middle = ""
    sql2 = """
    SELECT
      syscolumns.name AS Name ,
            systypes.name AS DataType        
    FROM
      syscolumns
    INNER JOIN systypes ON (
      syscolumns.xtype = systypes.xtype
            AND systypes.name <> '_default_' 
            AND systypes.name <> 'sysname'
            )                                       
    WHERE
      syscolumns.id = (
              SELECT
                      id
                    FROM
                      sysobjects
                    WHERE
                      name = '%s'
      )
    ORDER BY syscolumns.colid;    
    """ % table
    cursor.execute(sql2)
    field_records = cursor.fetchall()
    middle_list = []
    for field in field_records:
        fname, ftype = field
        middle_list.append("  \"%s\" %s" % (fname, ftype))
    ext_sql = head + ",\n".join(middle_list) + ',)\n' + end
    ext_sql = re.sub('\" .*char.*,', '" text,', ext_sql)
    ext_sql = re.sub('\" .*(numeric|int|float).*,', '" numeric,', ext_sql)
    ext_sql = re.sub('\" .*datetime.*,', '" timestamp,', ext_sql)
    ext_sql = ext_sql.replace(",)", ")")
    print(ext_sql)

cursor.close()
conn.close()
