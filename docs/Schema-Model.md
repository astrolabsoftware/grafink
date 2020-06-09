# Janusgraph Schema Model

This documents describes the schema modelling for JanusGraph, when alerts are being pushed by Grafink. It also briefly describes the algorithm to be followed for loading the data.

A single alert row has the following fields in HBase
```
key: schema_0.5.0_0.3.2
<ColumnFamily>:<ColumnName>:value
COLUMN                            CELL
d:cdsxmatch                      timestamp=1591692431847, value=string
d:mulens_class_1                 timestamp=1591692431847, value=string
d:mulens_class_2                 timestamp=1591692431847, value=string
d:rfscore                        timestamp=1591692431847, value=double
d:roid                           timestamp=1591692431847, value=integer
d:snnscore                       timestamp=1591692431847, value=double
fits:cutoutDifference            timestamp=1591692431847, value=binary
fits:cutoutScience               timestamp=1591692431847, value=binary
fits:cutoutTemplate              timestamp=1591692431847, value=binary
i:aimage                         timestamp=1591692431847, value=float
i:aimagerat                      timestamp=1591692431847, value=float
i:bimage                         timestamp=1591692431847, value=float
i:bimagerat                      timestamp=1591692431847, value=float
i:candid                         timestamp=1591692431847, value=long
i:chinr                          timestamp=1591692431847, value=float
i:chipsf                         timestamp=1591692431847, value=float
i:classtar                       timestamp=1591692431847, value=float
i:clrcoeff                       timestamp=1591692431847, value=float
i:clrcounc                       timestamp=1591692431847, value=float
i:clrmed                         timestamp=1591692431847, value=float
i:clrrms                         timestamp=1591692431847, value=float
i:dec                            timestamp=1591692431847, value=double
i:decnr                          timestamp=1591692431847, value=double
i:diffmaglim                     timestamp=1591692431847, value=float
i:distnr                         timestamp=1591692431847, value=float
i:distpsnr1                      timestamp=1591692431847, value=float
i:distpsnr2                      timestamp=1591692431847, value=float
i:distpsnr3                      timestamp=1591692431847, value=float
i:drb                            timestamp=1591692431847, value=float
i:drbversion                     timestamp=1591692431847, value=string
i:dsdiff                         timestamp=1591692431847, value=float
i:dsnrms                         timestamp=1591692431847, value=float
i:elong                          timestamp=1591692431847, value=float
i:exptime                        timestamp=1591692431847, value=float
i:fid                            timestamp=1591692431847, value=integer
i:field                          timestamp=1591692431847, value=integer
i:fink_broker_version            timestamp=1591692431847, value=string
i:fink_science_version           timestamp=1591692431847, value=string
i:fwhm                           timestamp=1591692431847, value=float
i:isdiffpos                      timestamp=1591692431847, value=string
i:jd                             timestamp=1591692431847, value=double
i:jdendhist                      timestamp=1591692431847, value=double
i:jdendref                       timestamp=1591692431847, value=double
i:jdstarthist                    timestamp=1591692431847, value=double
i:jdstartref                     timestamp=1591692431847, value=double
i:magap                          timestamp=1591692431847, value=float
i:magapbig                       timestamp=1591692431847, value=float
i:magdiff                        timestamp=1591692431847, value=float
i:magfromlim                     timestamp=1591692431847, value=float
i:maggaia                        timestamp=1591692431847, value=float
i:maggaiabright                  timestamp=1591692431847, value=float
i:magnr                          timestamp=1591692431847, value=float
i:magpsf                         timestamp=1591692431847, value=float
i:magzpsci                       timestamp=1591692431847, value=float
i:magzpscirms                    timestamp=1591692431847, value=float
i:magzpsciunc                    timestamp=1591692431847, value=float
i:mindtoedge                     timestamp=1591692431847, value=float
i:nbad                           timestamp=1591692431847, value=integer
i:ncovhist                       timestamp=1591692431847, value=integer
i:ndethist                       timestamp=1591692431847, value=integer
i:neargaia                       timestamp=1591692431847, value=float
i:neargaiabright                 timestamp=1591692431847, value=float
i:nframesref                     timestamp=1591692431847, value=integer
i:nid                            timestamp=1591692431847, value=integer
i:nmatches                       timestamp=1591692431847, value=integer
i:nmtchps                        timestamp=1591692431847, value=integer
i:nneg                           timestamp=1591692431847, value=integer
i:objectId                       timestamp=1591692431847, value=string
i:objectidps1                    timestamp=1591692431847, value=long
i:objectidps2                    timestamp=1591692431847, value=long
i:objectidps3                    timestamp=1591692431847, value=long
i:pdiffimfilename                timestamp=1591692431847, value=string
i:pid                            timestamp=1591692431847, value=long
i:programid                      timestamp=1591692431847, value=integer
i:programpi                      timestamp=1591692431847, value=string
i:publisher                      timestamp=1591692431847, value=string
i:ra                             timestamp=1591692431847, value=double
i:ranr                           timestamp=1591692431847, value=double
i:rb                             timestamp=1591692431847, value=float
i:rbversion                      timestamp=1591692431847, value=string
i:rcid                           timestamp=1591692431847, value=integer
i:rfid                           timestamp=1591692431847, value=long
i:schemavsn                      timestamp=1591692431847, value=string
i:scorr                          timestamp=1591692431847, value=double
i:seeratio                       timestamp=1591692431847, value=float
i:sgmag1                         timestamp=1591692431847, value=float
i:sgmag2                         timestamp=1591692431847, value=float
i:sgmag3                         timestamp=1591692431847, value=float
i:sgscore1                       timestamp=1591692431847, value=float
i:sgscore2                       timestamp=1591692431847, value=float
i:sgscore3                       timestamp=1591692431847, value=float
i:sharpnr                        timestamp=1591692431847, value=float
i:sigmagap                       timestamp=1591692431847, value=float
i:sigmagapbig                    timestamp=1591692431847, value=float
i:sigmagnr                       timestamp=1591692431847, value=float
i:sigmapsf                       timestamp=1591692431847, value=float
i:simag1                         timestamp=1591692431847, value=float
i:simag2                         timestamp=1591692431847, value=float
i:simag3                         timestamp=1591692431847, value=float
i:sky                            timestamp=1591692431847, value=float
i:srmag1                         timestamp=1591692431847, value=float
i:srmag2                         timestamp=1591692431847, value=float
i:srmag3                         timestamp=1591692431847, value=float
i:ssdistnr                       timestamp=1591692431847, value=float
i:ssmagnr                        timestamp=1591692431847, value=float
i:ssnamenr                       timestamp=1591692431847, value=string
i:ssnrms                         timestamp=1591692431847, value=float
i:sumrat                         timestamp=1591692431847, value=float
i:szmag1                         timestamp=1591692431847, value=float
i:szmag2                         timestamp=1591692431847, value=float
i:szmag3                         timestamp=1591692431847, value=float
i:tblid                          timestamp=1591692431847, value=long
i:tooflag                        timestamp=1591692431847, value=integer
i:xpos                           timestamp=1591692431847, value=float
i:ypos                           timestamp=1591692431847, value=float
i:zpclrcov                       timestamp=1591692431847, value=float
i:zpmed                          timestamp=1591692431847, value=float
```

Note that this schema is subject to evolution. There are currently 3 column families: `i` (original alert information), `d` (added values by Fink) and `fits` (binary gzipped FITS image). The above row will be translated into graph data model as follows:

<div id="link-properties" />
### Vertices

- Every alert (single row in hbase) will be modelled as a graph vertex.
- Following alert properties will be modelled as vertex properties hereby referred to as ```link properties```
    - Supernova Random Forest classification ```(d:rfscore)```
    - Supernova Bayesian Neural Network classification ```(d:snnscore)```
    - Microlensing Random Forest classification ```(d:mulens_class_i)```
    - Cross-match label ```(d:cdsxmatch)```
    - Asteroid/satellite identification ```(d:roid)```
    - Star/Galaxy separation ```(i:classtar)```
    - The object ID itself if we use individual alerts ```(i:objectId)```

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
