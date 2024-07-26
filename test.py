import rpyc

con = rpyc.connect('192.168.0.155',18861)

print(con.root.alive())

con.close()