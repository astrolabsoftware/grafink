# TestJanusGraph exploration

[2020/07/31] In this note, I explore the `TestJanusGraph` table. I use the grafink-shell (as of commit 9827ccb00aebc3218e1760fa87767357c5fc785f) whenever possible from my laptop (using sshuttle to connect to the remote table). I just updated the configuration file

```diff
+     host: "hbase-1.lal.in2p3.fr"
---
-     host: "127.0.0.1"
```

and simply launch the shell

```bash
./bin/grafink-shell --config conf/application.conf
...
grafink> g.V()

```

I sometimes encountered difficulty to use gremlin-scala (either limitation in the language, or just lack of documentation to write queries), hence I sometimes fall back to standard gremlin shell.

```bash
/path/to/janusgraph/bin/gremlin.sh
gremlin>  graph = JanusGraphFactory.build().set("storage.backend", "hbase").set("storage.hostname", "134.158.74.54").set("storage.hbase.table", "TestJanusGraph").open()
==>standardjanusgraph[hbase:[134.158.74.54]]
gremlin> g = graph.traversal()
==>graphtraversalsource[standardjanusgraph[hbase:[134.158.74.54]], standard]
```

In the following, the default shell is `grafink` but queries starting with `gremlin>` means I am using the default gremlin shell.

## Basics and definition

### How many vertices and edges in the table

How many vertices and and edges are in the table?

```java
grafink>g.V().count()
3019

grafink>g.V().outE("similarity").values("value").count().fold().next()
java.util.List[java.lang.Long] = [211888]
```

### Similarity computation

Based on the configuration file, the expression to compute the similarity is

```
similarityExp = "(rfscore AND snnscore) OR mulens OR cdsxmatch OR objectId OR roid"
```

But looking at [SimilarityExpParser.scala](https://github.com/astrolabsoftware/grafink/blob/c6ce1f5ca418a166cfc1ba3996a21086b4ac3fd4/src/main/scala/com/astrolabsoftware/grafink/processor/edgerules/SimilarityExpParser.scala#L107), I do not see the condition on `objectId` defined:

```scala
val scoreCond: (Column, Column) => Column = (score1, score2) => (score1 > 0.9) && (score2 > 0.9)
val cdsxmatchCond: (Column, Column) => Column = (cdsxmatch1, cdsxmatch2) =>
	(cdsxmatch1 =!= "Unknown") && (cdsxmatch1 === cdsxmatch2)
val roidCond: (Column, Column) => Column = (roid1, roid2) => (roid1 > 1) && (roid2 > 1)
val classtarCond: (Column, Column) => Column = (classtar1, classtar2) =>
	((classtar1 > 0.9) && (classtar2 > 0.9)) || ((classtar1 < 0.1) && (classtar2 < 0.1))

val mulensmlCond: (Column, Column, Column, Column) => Column =
	(mulens1_class_1, mulens1_class_2, mulens2_class_1, mulens2_class_2) =>
  		(mulens1_class_1 === "ML" && mulens1_class_2 === "ML") && (mulens2_class_1 === "ML" && mulens2_class_2 === "ML")
```

Where is the condition for `objectId` defined?

## Checking similarity

### Are there alerts with the same objectId?

Each alert has a unique identifier `candid`, and an identifier `objectId` which is shared among alerts coming from the same object on the sky. Hence, alerts with the same `objectId` has more chance to show similar properties and be linked.

```java
grafink>g.V()
	.groupCount()
	.by("objectId")
	.next()
	.asScala.filter{(k : (Any, java.lang.Long)) => k._2 > 1}
res37: collection.mutable.Map[Any, java.lang.Long] = Map(
  "ZTF19acmbvvv" -> 2L,
  "ZTF19acmbudf" -> 2L,
  "ZTF17aaaizsr" -> 2L,
  "ZTF19acmbwxo" -> 2L,
  "ZTF18aajhbfp" -> 3L,
  "ZTF19acmbwxp" -> 2L,
  "ZTF18aacsudg" -> 2L,
  "ZTF19acmcdpk" -> 2L,
  "ZTF19acmbwok" -> 2L,
  "ZTF18aaahybv" -> 2L,
  "ZTF19acmbwoj" -> 2L,
  "ZTF18aaaqgtp" -> 2L,
  "ZTF18aahesrm" -> 2L,
...
```

We can also see that the biggest families contain 3 alerts with the same `objectId`:

```java
grafink>g.V().groupCount().by("objectId").next().asScala.valuesIterator.max
res25: java.lang.Long = 3L

grafink>g.V().groupCount().by("objectId").next().asScala.filter{(k : (Any, java.lang.Long)) => k._2 == 3}
res34: collection.mutable.Map[Any, java.lang.Long] = Map(
  "ZTF18aaacpwn" -> 3L,
  "ZTF19acmbwzp" -> 3L,
  "ZTF18aacgqfy" -> 3L,
  "ZTF18aajhbfp" -> 3L,
  "ZTF18abvttnr" -> 3L
)
```

For one of them, let's try to compute manually the similarity score:


```scala
grafink>g.V().has("objectId", "ZTF18aaacpwn")
	.valueMap().fold().next().asScala
res38: collection.mutable.Buffer[java.util.Map[Object, Nothing]] = ArrayBuffer(
  {rfscore=[0.447], snnscore=[0.8499873280525208], mulens_class_1=[VARIABLE], mulens_class_2=[VARIABLE], cdsxmatch=[EB*], objectId=[ZTF18aaacpwn], roid=[0], classtar=[0.978]},
  {rfscore=[0.447], snnscore=[0.8499873280525208], mulens_class_1=[VARIABLE], mulens_class_2=[VARIABLE], cdsxmatch=[EB*], objectId=[ZTF18aaacpwn], roid=[0], classtar=[0.994]},
  {rfscore=[0.359], snnscore=[0.8325337171554565], mulens_class_1=[VARIABLE], mulens_class_2=[ML], cdsxmatch=[EB*], objectId=[ZTF18aaacpwn], roid=[0], classtar=[1.0]}
)
```

they should have a score of 2 thanks to `cdsxmatch` and `objectId` (note that `roid <= 1` should be not taken into account):

```
similarityExp = "(rfscore AND snnscore) OR mulens OR cdsxmatch OR objectId OR roid"
similarityExp = "(false) OR false OR true OR true OR false"
```


Let's have a look at it:

```java
gremlin> g.V().has("vertex", "objectId", "ZTF18aaacpwn").as("vertex_in")
	.outE().as("similarity")
	.otherV().has("objectId", "ZTF18aaacpwn").as("vertex_out")
	.select("vertex_in", "similarity", "vertex_out")
	.by(values("objectId", "value", "objectId"))
==>[vertex_in:ZTF18aaacpwn,similarity:2,vertex_out:ZTF18aaacpwn]
==>[vertex_in:ZTF18aaacpwn,similarity:2,vertex_out:ZTF18aaacpwn]
==>[vertex_in:ZTF18aaacpwn,similarity:2,vertex_out:ZTF18aaacpwn]
==>[vertex_in:ZTF18aaacpwn,similarity:2,vertex_out:ZTF18aaacpwn]
==>[vertex_in:ZTF18aaacpwn,similarity:2,vertex_out:ZTF18aaacpwn]
==>[vertex_in:ZTF18aaacpwn,similarity:2,vertex_out:ZTF18aaacpwn]
```

They indeed have a score of 2 - but why are there 6 results? This is normal - you have 3 vertices with the same objectId. Each has two outgoing edges pointing towards the two other vertices. One can remove duplicates:

```java
gremlin> g.V().has("vertex", "objectId", "ZTF18aaacpwn").as("vertex_in")
.outE().as("similarity")
.otherV().has("objectId", "ZTF18aaacpwn").as("vertex_out")
.dedup() // remove duplicates
.select("vertex_in", "similarity", "vertex_out")
.by(values("objectId", "value", "objectId"))
==>[vertex_in:ZTF18aaacpwn,similarity:2,vertex_out:ZTF18aaacpwn]
==>[vertex_in:ZTF18aaacpwn,similarity:2,vertex_out:ZTF18aaacpwn]
==>[vertex_in:ZTF18aaacpwn,similarity:2,vertex_out:ZTF18aaacpwn]
```

### What is the maximum similarity we can get in the dataset?

The highest similarity score between two vertices in the dataset is 2

```java
grafink>g.V().outE("similarity").values("value").max().fold().next()
res42: java.util.List[Nothing] = [2]

// better
grafink>g.V().outE("similarity").values[Long]("value").fold().next().asScala.max
res45: Long = 2L
```

Is that something expected?


| Type | Expected similarity | (rfscore AND snnscore) OR mulens OR cdsxmatch OR objectId OR roid
|---|----|-----|
| Supernovae | 3 | (true) OR false OR true OR true OR false |
| Microlensing | 2/3 | (false) OR true OR true/false OR true OR false |
| Catalogued object* | 2 | (false) OR false OR true OR true OR false |
| Asteroids | 1/2 | (false) OR false OR true/false OR false OR true |
| Random | 0/1 | (false) OR false OR true/false OR false OR false |
_* A catalogued object is an object already observed that is contained in astronomical catalogs. Typically variable stars that emit alerts perdiodically are in this category (such as ZTF18aaacpwn above)._

So based on this table, we would expect mostly Random, Asteroids, or catalogued objects in this dataset.

## Checking connectivity

### How many connections each vertex has?

We can compute the distribution of number of connection per vertex:

```java
// number of connection/vertex : number of vertex with this number of connection/vertex
gremlin> g.V().groupCount().by(out("similarity").count()).order(local).by(keys,incr)
==>[0:1689,1:103,2:15,3:10,4:9,5:14,6:18,8:3,9:11,10:11,11:12,15:1,20:4,21:9,22:8,
23:1,24:1,25:1,28:1,29:1,31:9,32:7,33:28,34:20,37:1,38:1,39:22,40:16,42:1,44:1,
45:1,47:1,50:2,52:1,54:1,58:1,60:1,62:1,63:1,64:9,66:18,67:1,68:1,69:4,70:33,
73:1,75:1,76:3,77:14,78:14,79:1,81:7,82:76,83:41,94:1,111:1,115:1,137:1,143:2,
145:1,148:1,151:1,160:1,161:1,169:2,171:1,173:2,174:1,176:1,177:1,178:1,179:1,
180:2,182:6,183:4,184:13,185:15,186:13,187:8,188:15,189:11,190:7,191:49,192:34,
199:1,205:1,208:1,209:1,214:2,215:1,216:2,219:1,222:2,223:2,226:1,227:2,228:2,
230:2,231:1,236:1,237:1,238:1,239:2,242:1,246:1,247:1,249:2,250:1,251:1,252:2,
253:1,254:4,256:1,257:2,258:3,260:2,261:3,263:9,264:27,265:25,266:27,267:10,
268:5,269:15,270:15,271:35,272:5,273:1,274:18,275:66,276:14,277:19,278:30,
279:10,280:7,281:4,282:25,283:46,284:13,285:10,286:68,290:1,292:1,357:1]
```

Half of the vertices (1689) has no connection (i.e. they have similarity of 0). The most connected vertex has 357 connections. Let's inspect this highly connected vertex:

```java
gremlin> g.V().where(out("similarity").count().is(eq(357))).valueMap()
==>[rfscore:[0.0],snnscore:[0.5770244598388672],mulens_class_1:[],mulens_class_2:[],cdsxmatch:[EB*],objectId:[ZTF18aabyhqi],roid:[2],classtar:[0.997]]
```

Hum, he has potentially three properties that can be used to compute a non-zero similarity: `cdsxmatch`, `objectId`, and `roid`. This is a glitch as `cdsxmatch=EB*` (variable star) and `roid=2` (flagged as asteroid) are not compatible, so potentially this alert is linked to two big clusters: the catalogued stars, and the asteroids. We will see later that those are moreover the biggest cluster in our dataset. I need to make sure this does not happen in the future! Typically, this means our [algorithm](https://github.com/astrolabsoftware/fink-science/tree/master/fink_science/asteroids) to detect asteroids is somewhat not completely reliable.

### CDS cross-match clusters

Let's have a look at catalogued objects. 

```java
gremlin> g.V().groupCount().by("cdsxmatch").order(local).by(values,decr)
==>[Unknown:1856,EB*:287,RRLyr:276,QSO:193,Seyfert_1:84,Orion_V*:71,TTau*:41,
Star:35,YSO:34,BLLac:23,Galaxy:12,RSCVn:11,CataclyV*:10,SN:7,Em*:7,Flare*:7,
Mira:6,Radio:6,DwarfNova:5,LPV*:5,C*:4,PulsV*delSct:4,V*:3,X:3,AGN:3,
EB*Algol:3,Candidate_RRLyr:3,Candidate_SN*:2,Blue:2,EllipVar:1,
low-mass*:1,Candidate_TTau*:1,Seyfert_2:1,Candidate_YSO:1,*inNeb:1,
BLLac_Candidate:1,IR:1,Blazar:1,Cepheid:1,AMHer:1,HB*:1,
HotSubdwarf:1,S*:1,Candidate_CV*:1,WD*:1]
```

The object the most represented is `Unknown`, meaning the objects not catalogued. Then we have classes like `EB*` and `RRLyr` which are very well known variable stars. Good, this is somewhat expected. Let's have a look at clusters now, that is which class produces the highest number of edges:

```java
gremlin> g.V().group().by("cdsxmatch").by(out("similarity").count()).order(local).by(values,decr)
==>[EB*:79896,RRLyr:73082,QSO:35845,Seyfert_1:6881,Unknown:6462,Orion_V*:4718,
TTau*:1594,Star:1248,YSO:1086,BLLac:469,Galaxy:132,RSCVn:110,CataclyV*:90,
SN:42,Em*:40,Flare*:38,Radio:32,Mira:30,LPV*:20,DwarfNova:18,C*:12,
PulsV*delSct:12,V*:6,X:6,EB*Algol:6,Candidate_RRLyr:6,Candidate_SN*:2,
Blue:2,AGN:2,BLLac_Candidate:1,EllipVar:0,low-mass*:0,
Candidate_TTau*:0,Seyfert_2:0,Candidate_YSO:0,*inNeb:0,IR:0,
Blazar:0,Cepheid:0,AMHer:0,HB*:0,HotSubdwarf:0,S*:0,Candidate_CV*:0,WD*:0]
```

There are "only" 287 objects catalogued as `EB*` but they generate almost 80,000 edges! On the contrary, there are about 1,900 `Unknown`, but they are poorly linked with the rest (6,500 edges). If I keep only the ones with similarity of 2:

```java
gremlin> g.E().where(values("value").is(eq(2))).outV().group().by("cdsxmatch").by(out("similarity").count()).order(local).by(values,decr)
==>[RRLyr:10009,EB*:9552,QSO:1503,Seyfert_1:1318,
Unknown:176,BLLac:167,Galaxy:44,RSCVn:20,Radio:10,PulsV*delSct:6]
```

### Most linked objects

The most linked objects in the dataset are:

```java
gremlin> g.V().group().by("objectId").by(out("similarity").count()).order(local).by(values,decr)
==>[ZTF18aaacpwn:837,ZTF18aabjgci:572,ZTF18aaacxfc:572,ZTF18aabjmfe:572,
ZTF18aaabzcb:571,ZTF18aaahybv:568,ZTF18aaahoed:564,ZTF18abvttnr:563,
ZTF18aaadljo:562,ZTF17aaajbet:561,ZTF18aaabyqu:561,ZTF18aaabxvk:560,
ZTF18aaacxty:559,ZTF17aaclxsh:558,ZTF18aahouhg:550,ZTF17aacvxlg:546,
ZTF17aaaizsr:546,ZTF18aaacpww:541,ZTF18aaaqgtp:541,ZTF18aaacsxo:540,
ZTF18aaabyaj:540,ZTF17aaajffd:531,ZTF17aaairdu:528,ZTF18aaafvfg:522,
ZTF18aaacmdt:516,ZTF18aabklbs:483,ZTF18aaacmyn:448,ZTF17aaajahq:427,
ZTF19abyjnwt:377,ZTF18aabyhqi:357,ZTF18aabjigr:292,ZTF18acrultp:290,
ZTF18aabjfrv:286, 
...
```

let's have a look at the first one:

```java
gremlin> g.V().has("objectId", "ZTF18aaacpwn").valueMap()
==>[rfscore:[0.447],snnscore:[0.8499873280525208],mulens_class_1:[VARIABLE],mulens_class_2:[VARIABLE],cdsxmatch:[EB*],objectId:[ZTF18aaacpwn],roid:[0],classtar:[0.978]]
==>[rfscore:[0.447],snnscore:[0.8499873280525208],mulens_class_1:[VARIABLE],mulens_class_2:[VARIABLE],cdsxmatch:[EB*],objectId:[ZTF18aaacpwn],roid:[0],classtar:[0.994]]
==>[rfscore:[0.359],snnscore:[0.8325337171554565],mulens_class_1:[VARIABLE],mulens_class_2:[ML],cdsxmatch:[EB*],objectId:[ZTF18aaacpwn],roid:[0],classtar:[1.0]]
```

Three alerts have the same `objectId`, and they all belong to the `EB*` class (most represented), hence the high connectivity.

### Supernovae

We suspected there were no supernovae because we have no edges with similarity above 2. But let's check manually if that's the case based on the supernovae classifier scores:

```java
grafink>g.V().has("snnscore", gt(0.9)).has("rfscore", gt(0.9)).valueMap()
==>null 
```

### Microlensing

We suspected there were no microlensing because we have no edges with similarity above 2. But let's check manually if that's the case based on the supernovae classifier scores:

```java
gremlin> g.V().has("mulens_class_1", "ML").has("mulens_class_2", "ML").valueMap()
==>[rfscore:[0.447],snnscore:[0.829960823059082],mulens_class_1:[ML],mulens_class_2:[ML],cdsxmatch:[RRLyr],objectId:[ZTF18aaaghqe],roid:[0],classtar:[0.988]]
==>[rfscore:[0.447],snnscore:[0.7523323893547058],mulens_class_1:[ML],mulens_class_2:[ML],cdsxmatch:[Unknown],objectId:[ZTF19abymvoy],roid:[0],classtar:[0.983]]
==>[rfscore:[0.447],snnscore:[0.7193341851234436],mulens_class_1:[ML],mulens_class_2:[ML],cdsxmatch:[RRLyr],objectId:[ZTF17aaappoo],roid:[0],classtar:[0.992]]
==>[rfscore:[0.447],snnscore:[0.7705780863761902],mulens_class_1:[ML],mulens_class_2:[ML],cdsxmatch:[Unknown],objectId:[ZTF19abymvoy],roid:[0],classtar:[0.983]]
==>[rfscore:[0.447],snnscore:[0.6853560209274292],mulens_class_1:[ML],mulens_class_2:[ML],cdsxmatch:[EB*],objectId:[ZTF18acrultp],roid:[0],classtar:[0.954]]
==>[rfscore:[0.379],snnscore:[0.056191038340330124],mulens_class_1:[ML],mulens_class_2:[ML],cdsxmatch:[Unknown],objectId:[ZTF18acrukmn],roid:[0],classtar:[1.0]]
==>[rfscore:[0.28],snnscore:[0.7540108561515808],mulens_class_1:[ML],mulens_class_2:[ML],cdsxmatch:[EB*],objectId:[ZTF18aabjigr],roid:[0],classtar:[0.945]]
==>[rfscore:[0.655],snnscore:[0.7129868865013123],mulens_class_1:[ML],mulens_class_2:[ML],cdsxmatch:[RRLyr],objectId:[ZTF17aaajkxp],roid:[0],classtar:[0.983]]
==>[rfscore:[0.447],snnscore:[0.28106093406677246],mulens_class_1:[ML],mulens_class_2:[ML],cdsxmatch:[Star],objectId:[ZTF17aaarspn],roid:[0],classtar:[0.984]] 
```

In each we have the `mulens_class_*` which are activated, and potentially `objectId` and `cdsxmatch`. So why aren't we at similarity 3? First, there are only `ZTF19abymvoy` which appears twice (hence rising the similarity), but unfortunately, both alerts have `cdsxmatch=Unknown` which not considered for similarity. Hence, all possible combination between all alerts listed above lead to a similarity score at most of 2.

### Asteroids

The asteroid classification is

```java
gremlin> g.V().groupCount().by("roid").order(local).by(values,decr)
==>[0:1635,1:1301,3:74,2:9]
```

Only scores above 1 are considered, so that concerns very alerts (9 alerts have score of 2, and 74 alerts have score of 3). Let's have a look at the ones with a score of 2:

```java
gremlin> g.V().has("roid", 2).valueMap()
==>[rfscore:[0.0],snnscore:[0.4936774671077728],mulens_class_1:[],mulens_class_2:[],cdsxmatch:[Unknown],objectId:[ZTF19acmbnun],roid:[2],classtar:[0.999]]
==>[rfscore:[0.0],snnscore:[0.5770244598388672],mulens_class_1:[],mulens_class_2:[],cdsxmatch:[EB*],objectId:[ZTF18aabyhqi],roid:[2],classtar:[0.997]]
==>[rfscore:[0.0],snnscore:[0.4939023554325104],mulens_class_1:[],mulens_class_2:[],cdsxmatch:[Unknown],objectId:[ZTF19aclxgig],roid:[2],classtar:[0.984]]
==>[rfscore:[0.0],snnscore:[0.6197949051856995],mulens_class_1:[],mulens_class_2:[],cdsxmatch:[Unknown],objectId:[ZTF18aclchwa],roid:[2],classtar:[0.997]]
==>[rfscore:[0.0],snnscore:[0.656506359577179],mulens_class_1:[],mulens_class_2:[],cdsxmatch:[Star],objectId:[ZTF18aaqchlp],roid:[2],classtar:[1.0]]
==>[rfscore:[0.0],snnscore:[0.4925912320613861],mulens_class_1:[],mulens_class_2:[],cdsxmatch:[Unknown],objectId:[ZTF18acewvmc],roid:[2],classtar:[0.976]]
==>[rfscore:[0.0],snnscore:[0.5102506279945374],mulens_class_1:[],mulens_class_2:[],cdsxmatch:[Unknown],objectId:[ZTF18aaazijj],roid:[2],classtar:[0.6]]
==>[rfscore:[0.0],snnscore:[0.5245912075042725],mulens_class_1:[],mulens_class_2:[],cdsxmatch:[Unknown],objectId:[ZTF18actauqq],roid:[2],classtar:[0.989]]
==>[rfscore:[0.0],snnscore:[0.5039762854576111],mulens_class_1:[],mulens_class_2:[],cdsxmatch:[Unknown],objectId:[ZTF19acjhvvu],roid:[2],classtar:[0.864]]

gremlin> g.V().has("roid", 2).outE("similarity").count()
==>1035
```
There are 9 alerts with `roid=2`, but they create 1035 edges! Let's have a look at which alert creates the biggest number of edges:

```java
gremlin> g.V().has("roid", 2).as("r").outE("similarity").select("r").groupCount().by(values("objectId")).order(local).by(values,decr)
==>[ZTF18aabyhqi:357,ZTF18aaqchlp:115,ZTF19acjhvvu:82,ZTF18actauqq:82,ZTF18aaazijj:82,ZTF18acewvmc:81,ZTF18aclchwa:81,ZTF19aclxgig:78,ZTF19acmbnun:77]
```

With no surprises, `ZTF18aabyhqi` has the highest number of connections, and it is also classified as `EB*` (biggest cluster). This means we should update the asteroid module to reject this as an asteroid!

```java
gremlin> g.V().has("objectId", "ZTF18aabyhqi").outE("similarity").groupCount().by("value")
==>[1:357]
```

All connections for `ZTF18aabyhqi` have similarity of 1, that is they come from `cdsxmatch=EB*` only.


### Recommendation

Recommendation to better disantangle alerts:
 
- add the column `candid`
- add the column `candidate.jd`
- add the column `magpsf`
- add the column `sigmapsf`

Make a better [asteroid algorithm](https://github.com/astrolabsoftware/fink-science/tree/master/fink_science/asteroids) to remove false positive (alerts with CDS classification cannot be asteroid for example).

We need more properties to compute similarity. Ideally this will come from new science modules in preparation.
