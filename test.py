import rpyc

con = rpyc.connect('localhost',18862)

print(con.root.move('./test.txt','./x.txt'))

con.close()