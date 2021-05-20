file = open('./zcc.txt', 'r')
d = {}
line = file.read().split("\n")
for i in line:
    nodes = i.split(" ")
    if(len(nodes)!=2):
        continue
    node1 = (nodes[0])
    node2 = (nodes[1])
    if(node1 not in d.keys()):
        d[node1] = []
    if(node2 not in d.keys()):
        d[node2] = []
    d[node1].append(node2)
    d[node2].append(node1)

file_w = open('out.txt','w')

for key,value in d.items():
    s = key+" "+(",".join(value))
    file_w.write(s)
    file_w.write("\n")