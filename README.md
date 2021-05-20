# PSCAN in Hadoop
A graph clustering algorithm called SCAN implemented in Apache Hadoop by a method called as PSCAN. This algorithm is linear in nature and has complexity of O(m), where m is the number of edges in the graph. <br />

This is an implementation of the paper <br />
> Zhao, W., Martha, V., & Xu, X. (2013, March). PSCAN: a parallel Structural clustering algorithm for big networks in MapReduce. In Advanced Information Networking and Applications (AINA), 2013 IEEE 27th International Conference on (pp. 862-869). IEEE <br />

which is a MapReduce implementation of the paper <br />

> X.Xu, N.Yuruk, Z. Feng, T. Schweiger. SCAN: a structural clustering algorithm for networks,Proceedings of the 13th ACM SIGKDD international conference on Knowledge discovery and data mining, pp. 824-833, 2007. <br />

**Note:** There are slight changes in my code which are not present in the PSCAN paper, but the underlying results are same.

## Content
1. [Flow of the program](#flow-of-the-program)
2. [Preprocessing Graph](#preprocessing-graph)
3. [Parameters of input](#parameters-of-input)
4. [Output in Hadoop](#output-in-hadoop)

## Flow of the Program
Let's assume that we have a graph such as this-

![sample_graph](https://user-images.githubusercontent.com/59285634/118945196-dd96f400-b972-11eb-8d42-d90331006848.png)

The input for this graph is assumed to be in the format of node-adjacency list.
``` 
1 2,6
2 1,3,5,6
3 2,4
4 3
5 2,6
6 1,2,5,7,8
7 6
8 6
```

If the graph is in form of edges, check out the [Preprocessing](#preprocessing-graph) section. 

We first implement a MapReduce program, to calculate the structural similarity of all the edges in the graph. If this value is above a certain threshold (default=0.5), then only we output those edges, otherwise we dont consider them. The output for this looks like this -

```
1,2 0.5168..
1,3 0.51.. 
```

Next, we take this output and then process this in the form of ``<vertex, structure_info>`` form of pairs. For this we implement another MapReduce program. The output for this looks like -

```
1 1:1:2
2 1:2:3,4
```

The second parameter of the input is called the structure_info. This contains the status, label and adjacency list of the vertex. Status tells us if the vertex is active(1) or not(0), the label tells us the cluster group which the node belongs to. Initially, the label is the vertex itself.

This input is the processed by the next MapReduce which is an iterative process (refer to the paper for more information). This iteration is dependent on the diameter of the graph, i.e., if the diameter is 4, this program will iterate 4 times. The output looks like the one above, but the status of all the nodes should be 0 by the end of all iterations.

This output is then given to the last MapReduce program which basically groups all clusters together. This looks like -

```
1 1,2,3,4
6 6,7,8
```

## Preprocessing Graph
If the graph is given in the form of edge input format, i.e.,

```
1 2
2 3
3 4
```

then just run the ``graph_procc.py`` file given in the repo. Change the input name of the file, with whatever file you have and set output accordingly.

## Parameters of Input

While running Hadoop 3, the input to run the program should be given in the format - 

`` hadoop jar pscan.jar driver <directory-to-input-file> <directory-to-output-file> <diameter-of-graph>``

As I stated before, the diameter of the graph is a new addition that I have made for input.

## Output in Hadoop
