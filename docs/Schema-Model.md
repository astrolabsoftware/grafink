# Janusgraph Schema Model

This documents describes the schema modelling for JanusGraph, when alerts are being pushed by Grafink. It also briefly describes the algorithm to be followed for loading the data.

There are currently 4 column families in the main HBase table:
- `i`: original alert information,
- `d`: added values by Fink,
- `b`: binary data (e.g. binary gzipped FITS image),
- `a`: for annotations (initially empty).

A single alert row has the following fields in HBase (fink-broker version 0.7.0, fink-science version 0.3.6):

```
hbase(main):006:0> get "test_portal_tiny.3", "schema_0.7.0_0.3.6"
COLUMN                            CELL
 b:cutoutDifference_stampData     timestamp=1598256131226, value=fits/image
 b:cutoutScience_stampData        timestamp=1598256131226, value=fits/image
 b:cutoutTemplate_stampData       timestamp=1598256131226, value=fits/image
 d:cdsxmatch                      timestamp=1598256131226, value=string
 d:mulens_class_1                 timestamp=1598256131226, value=string
 d:mulens_class_2                 timestamp=1598256131226, value=string
 d:nalerthist                     timestamp=1598256131226, value=integer
 d:rfscore                        timestamp=1598256131226, value=double
 d:roid                           timestamp=1598256131226, value=integer
 d:snn_sn_vs_all                  timestamp=1598256131226, value=double
 d:snn_snia_vs_nonia              timestamp=1598256131226, value=double
 i:aimage                         timestamp=1598256131226, value=float
 i:aimagerat                      timestamp=1598256131226, value=float
 i:bimage                         timestamp=1598256131226, value=float
 i:bimagerat                      timestamp=1598256131226, value=float
 i:candid                         timestamp=1598256131226, value=long
 i:chinr                          timestamp=1598256131226, value=float
 i:chipsf                         timestamp=1598256131226, value=float
 i:classtar                       timestamp=1598256131226, value=float
 i:clrcoeff                       timestamp=1598256131226, value=float
 i:clrcounc                       timestamp=1598256131226, value=float
 i:clrmed                         timestamp=1598256131226, value=float
 i:clrrms                         timestamp=1598256131226, value=float
 i:dec                            timestamp=1598256131226, value=double
 i:decnr                          timestamp=1598256131226, value=double
 i:diffmaglim                     timestamp=1598256131226, value=float
 i:distnr                         timestamp=1598256131226, value=float
 i:distpsnr1                      timestamp=1598256131226, value=float
 i:distpsnr2                      timestamp=1598256131226, value=float
 i:distpsnr3                      timestamp=1598256131226, value=float
 i:drb                            timestamp=1598256131226, value=float
 i:drbversion                     timestamp=1598256131226, value=string
 i:dsdiff                         timestamp=1598256131226, value=float
 i:dsnrms                         timestamp=1598256131226, value=float
 i:elong                          timestamp=1598256131226, value=float
 i:exptime                        timestamp=1598256131226, value=float
 i:fid                            timestamp=1598256131226, value=integer
 i:field                          timestamp=1598256131226, value=integer
 i:fink_broker_version            timestamp=1598256131226, value=string
 i:fink_science_version           timestamp=1598256131226, value=string
 i:fwhm                           timestamp=1598256131226, value=float
 i:isdiffpos                      timestamp=1598256131226, value=string
 i:jd                             timestamp=1598256131226, value=double
 i:jdendhist                      timestamp=1598256131226, value=double
 i:jdendref                       timestamp=1598256131226, value=double
 i:jdstarthist                    timestamp=1598256131226, value=double
 i:jdstartref                     timestamp=1598256131226, value=double
 i:magap                          timestamp=1598256131226, value=float
 i:magapbig                       timestamp=1598256131226, value=float
 i:magdiff                        timestamp=1598256131226, value=float
 i:magfromlim                     timestamp=1598256131226, value=float
 i:maggaia                        timestamp=1598256131226, value=float
 i:maggaiabright                  timestamp=1598256131226, value=float
 i:magnr                          timestamp=1598256131226, value=float
 i:magpsf                         timestamp=1598256131226, value=float
 i:magzpsci                       timestamp=1598256131226, value=float
 i:magzpscirms                    timestamp=1598256131226, value=float
 i:magzpsciunc                    timestamp=1598256131226, value=float
 i:mindtoedge                     timestamp=1598256131226, value=float
 i:nbad                           timestamp=1598256131226, value=integer
 i:ncovhist                       timestamp=1598256131226, value=integer
 i:ndethist                       timestamp=1598256131226, value=integer
 i:neargaia                       timestamp=1598256131226, value=float
 i:neargaiabright                 timestamp=1598256131226, value=float
 i:nframesref                     timestamp=1598256131226, value=integer
 i:nid                            timestamp=1598256131226, value=integer
 i:nmatches                       timestamp=1598256131226, value=integer
 i:nmtchps                        timestamp=1598256131226, value=integer
 i:nneg                           timestamp=1598256131226, value=integer
 i:objectId                       timestamp=1598256131226, value=string
 i:objectidps1                    timestamp=1598256131226, value=long
 i:objectidps2                    timestamp=1598256131226, value=long
 i:objectidps3                    timestamp=1598256131226, value=long
 i:pdiffimfilename                timestamp=1598256131226, value=string
 i:pid                            timestamp=1598256131226, value=long
 i:programid                      timestamp=1598256131226, value=integer
 i:programpi                      timestamp=1598256131226, value=string
 i:publisher                      timestamp=1598256131226, value=string
 i:ra                             timestamp=1598256131226, value=double
 i:ranr                           timestamp=1598256131226, value=double
 i:rb                             timestamp=1598256131226, value=float
 i:rbversion                      timestamp=1598256131226, value=string
 i:rcid                           timestamp=1598256131226, value=integer
 i:rfid                           timestamp=1598256131226, value=long
 i:schemavsn                      timestamp=1598256131226, value=string
 i:scorr                          timestamp=1598256131226, value=double
 i:seeratio                       timestamp=1598256131226, value=float
 i:sgmag1                         timestamp=1598256131226, value=float
 i:sgmag2                         timestamp=1598256131226, value=float
 i:sgmag3                         timestamp=1598256131226, value=float
 i:sgscore1                       timestamp=1598256131226, value=float
 i:sgscore2                       timestamp=1598256131226, value=float
 i:sgscore3                       timestamp=1598256131226, value=float
 i:sharpnr                        timestamp=1598256131226, value=float
 i:sigmagap                       timestamp=1598256131226, value=float
 i:sigmagapbig                    timestamp=1598256131226, value=float
 i:sigmagnr                       timestamp=1598256131226, value=float
 i:sigmapsf                       timestamp=1598256131226, value=float
 i:simag1                         timestamp=1598256131226, value=float
 i:simag2                         timestamp=1598256131226, value=float
 i:simag3                         timestamp=1598256131226, value=float
 i:sky                            timestamp=1598256131226, value=float
 i:srmag1                         timestamp=1598256131226, value=float
 i:srmag2                         timestamp=1598256131226, value=float
 i:srmag3                         timestamp=1598256131226, value=float
 i:ssdistnr                       timestamp=1598256131226, value=float
 i:ssmagnr                        timestamp=1598256131226, value=float
 i:ssnamenr                       timestamp=1598256131226, value=string
 i:ssnrms                         timestamp=1598256131226, value=float
 i:sumrat                         timestamp=1598256131226, value=float
 i:szmag1                         timestamp=1598256131226, value=float
 i:szmag2                         timestamp=1598256131226, value=float
 i:szmag3                         timestamp=1598256131226, value=float
 i:tblid                          timestamp=1598256131226, value=long
 i:tooflag                        timestamp=1598256131226, value=integer
 i:xpos                           timestamp=1598256131226, value=float
 i:ypos                           timestamp=1598256131226, value=float
 i:zpclrcov                       timestamp=1598256131226, value=float
 i:zpmed                          timestamp=1598256131226, value=float
1 row(s)
Took 0.0752 seconds
```

Note that this schema is subject to evolution. The above row will be translated into graph data model as follows:

<div id="link-properties" />
### Vertices

- Every alert (single row in hbase) will be modelled as a graph vertex.
- Following alert properties will be modelled as vertex properties hereby referred to as ```link properties```
    - `xmatch`: returns the SIMBAD closest counterpart of an alert, based on position.
    - `random_forest_snia`: returns the probability of an alert to be a SNe Ia using a Random Forest Classifier (binary classification)
    - `snn`: returns the probability of an alert to be a SNe Ia using [SuperNNova](https://github.com/supernnova/SuperNNova). Two pre-trained models:
    - `snn_snia_vs_nonia`: Ia vs core-collapse SNe
    - `snn_sn_vs_all`: SNe vs. anything else (variable stars and other categories in the training)
    - `microlensing`: returns the predicted class (among microlensing, variable star, cataclysmic event, and constant event) & probability of an alert to be a microlensing event in each band using [LIA](https://github.com/dgodinez77/LIA).
    - `asteroids`: Determine if the alert is an asteroid (experimental).
    - `nalerthist`: Number of detections contained in each alert (current+history). Upper limits are not taken into account.


**TODO: describe TwoModeClassifier and SameValueClassifier logic.**

Each alert has two ids: ```objectId``` and ```candid```.
```candid``` is unique per alert.
```objectId``` is shared among alerts that point to the same astrophysical objects.
Say, if a supernova is observed at two different times ```t1```  and ```t2```,
the alerts emitted would have the same ```objectId```,
but different times (```jd``` ) and different ```candid```.

### Edges
- Create an edge between all the vertices that have same ```link``` properties.
- Definition of 'same' property will vary, and will be controlled by some rules, for eg:
   - String properties would be considered same if they are equal
   - ML properties might be considered same if they are below a threshold or within the same range etc.

### Data Insertion Algorithm (might change based on answers to the questions)

- Start by creating the schema in janusgraph if it does not exist
   This will involve creating
   - Vertex Labels
     Each vertex will have one label which is a kind of its 'type'.
   - Edge Labels (not sure what will be the edge label exactly)
   - Property Keys
        - Vertex property ```Rowkey```
        - All ```link``` properties as vertex properties as described above.
- Read all the rows, group by the interesting columnnames (link properties). Vertices in each group become ```cliques``` of the graph, since each of them needs to be connected to all the others in the group. Load all these as vertices of the graph and add the label and properties.
- Then for each of these groups:
   - Search for all vertices with the same link properties.
   - For each vertex in the group, add edges to all the searched vertices.
- Create indexes

### Questions
1. How to decide edge directions? What will be the edge label?

It seems it is better to go with directed edges since support for unidirected edges is limited (https://docs.janusgraph.org/advanced-topics/advschema/)

Ans: Edges will be Unidirectional

2. Do we connect the vertices because they have the same property or the property and their values must match as well?

Ans: Depends on property and rules associated with it (or its type)

3. What properties to create indices on?

Ans: TBD

4. Do we create the edges only for vertices being loaded for the current batch job or we want to connect the new alerts (add edges) with the existing alerts (vertices) in Janusgraph?

Ans: We want to connect old vertices as well.
This might be very slow as we have to return a lot of vertices based on our ```link``` properties and add edges to each of them.
