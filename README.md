LASH
===================
----------
#Introduction
------------
LASH is a scalable, distributed algorithm for mining sequential patterns in the presence of hierarchies. LASH takes as input a collection of sequences, each composed of items from some application-specific vocabulary. In contrast to traditional approaches to sequence mining, the items in the vocabulary are arranged in a hierarchy: both input sequences and sequential patterns may consist of items from different levels of the hierarchy. For example, the individual words in a text document can be arranged in a *syntactic hierarchy*: words (e.g., "lives") generalize to their lemma ("live"), which in turn generalize to their respective part-of-speech tag ("verb"). Products in sequences of customer transactions also form a natural *product hierarchy*, e.g. "Canon EOS 70D" may generalize to "digital camera", which generalizes to "photography", which in turn generalizes to "electronics". Entities such as persons can be arranged in "semantic hierarchies"; e.g., "Barack Obama" may generalize to "politician", "person", "entity". 
This generalization allows us to find sequences that would otherwise be hidden. For example, in the context of text
mining, such patterns include generalized *n*-grams (the ADJ house) or typed relational patterns (PERSON lives in CITY). In both cases, the patterns do not actually occur in the input data, but are useful for language modeling or information extraction tasks. Hierarchies can also be exploited when mining market-basket data or product sequences. For example, users may first buy some camera, then some photography book, and finally some flash. A detailed description about LASH can be found in the [SIGMOD 2015 paper].

[SIGMOD 2015 paper]: http://dws.informatik.uni-mannheim.de/fileadmin/shared/pi1/kbeedkar/publications/beedkar15lash.pdf

----------


#LASH Overview
------------------
Given a collection of input sequences of items and an item hierarchy, LASH mines all generalized sub-sequences (along with their frequencies) that:
- Occur in at least σ ≥ 2 sequences (support threshold)
- Have length at most λ ≥ 2 (length threshold)
- Have gap at most γ ≥ 0 between consecutive items (gap threshold)

#Building LASH
-----------------
1. Prerequisites for building LASH
	- Java JDK 1.6 or higher
	- Maven 3.0 or higher
	- Hadoop (for distributed processing)
2. Set the environment variables
```sh
$ export JAVA_HOME=/location/of/java/
$ # for running on a hadoop cluster
$ export HADOOP_HOME=/location/of/Hadoop/
```
3. Check out the LASH source code to some directory, we will call it here has LASH_HOME
4. Compiling
```sh
$ cd LASH_HOME
$ mvn clean install
```

#Running LASH
-----------
##Input Format
####Input sequences
The input sequnce file(s) should have the following format:
```
seqId item_1 item_2 item_3 . . . item_n
```
i.e., the first token of each line (whitespace delimited) becomes the sequence id, and all additional text of the line is interpreted as a sequence of items. 

####Hierarchies
Item hierarchies should be specified in a *single* file using the following format:
```
item <tab> parent
```
i.e., each line (tab delimited) contains an item and its generalization (parent item). Note that each item in the hierarchy should have only one parent, i.e., the hierarchy should form a forest.

##Sequential Mode
The sequential version of the algorithm runs locally on a single machine. 
```sh
$ LASH_HOME/bin/lash -i path/to/input/ -H path/to/hierarchy/ -o path/to/output -s σ -g γ -l λ -m s
```

##Distributed Mode
To run LASH on a Hadoop cluster, issue the (d)istributed mode.
```sh
$ LASH_HOME/bin/lash -i path/to/input/ -H path/to/hierarchy -o path/to/output/ -s σ -g γ -l λ -m d
```	
>  Note: 
> `path/to/input/` should point to the **directory** containg input sequence file(s).
> `path/to/hierarchy` should point to the **hierarchy file** (the hierarchy file should not be in the same directory as input sequence files).
> `path/to/output/` should point to the **directory** where output will be written.


##Output Format
```
sequence <tab> frequency
```

##Example
Assume that we have the following product sequences in the input file `SAMPLE_INPUT/input.txt`
```
s1 Cannon_EOS_70D The_Digital_Photography SanDisk_Extreme_32GB Dolica_GX600B200
s2 Nikon_D5300 On_Photography Ravelli_APGL5
s3 Canon_SX410 The_Art_of_Photography Kingston_8_GB_microSDHC
```
and a product hierarchy in the file `hierarchy.txt`
```
Cannon_EOS_70D          DSLR_Camera
Nikon_D5300             DSLR_Camera
Canon_SX410             PointShoot_Camera
DSLR_Camera             Camera
PointShoot_Camera       Camera
Dolica_GX600B200        Tripod
Ravelli_APGL5           Tripod
The_Digital_Photography Photography_Book
On_Photography          Photography_Book
The_Art_of_Photography  Photography_Book
SanDisk_Extreme_32GB    Flash
Kingston_8_GB_microSDHC Flash
```
Run the following command to find all **generalized sequences** with minimum support=2, maximum length=3 and maximum gap=1.
```sh
bin/lash -i SAMPLE_INPUT/ -H hierarchy.txt -o SAMPLE_OUPUT/ -s 2 -g 1 -l 3 -m s
```
Sample output:
```
Camera Photography_Book 	3
Camera Photography_Book Tripod 	2
Camera Photography_Book Flash 	2
Camera Flash 	2
DSLR_Camera Photography_Book 	2
DSLR_Camera Photography_Book Tripod 	2
Photography_Book Tripod 	2
Photography_Book Flash 	2
```
##Command line options
```
 -g,--gap <arg>           maximum gap.
 -H,--hierarchy <arg>     hierarchy path.
 -h,--help                show help.
 -i,--input <arg>         input path.
 -k,--keepFiles <arg>     translated input path.
 -l,--length <arg>        maximum length.
 -m,--mode <arg>          execution mode: (s)equential or (d)istributed.
 -n,--numReducers <arg>   number of reducers.
 -o,--output <arg>        output path.
 -r,--resume <arg>        translated input path.
 -s,--support <arg>       minimum support.
 ```
