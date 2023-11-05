import rpyc

con = rpyc.connect('localhost',18862)

print(con.root.f())

con.close()