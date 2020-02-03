import happybase

table_name = 'air_quality'
connection = happybase.Connection('localhost')
table = connection.table(table_name)
file_out = open("keys.txt", "w")
for key, data in table.scan():
    file_out.write(key.decode('utf-8') + "\n")
file_out.close()