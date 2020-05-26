# Janusgraph Schema Model

This documents describes the schema modelling for JanusGraph, when alerts are being pushed by Grafink. It also briefly describes the algorithm to be followed for loading the data.

A single alert row has the following fields in hbase

Note that this schema is subject to evolution.
```
key: ZTF19acmbtud_2458789.0279514_128.2831039_15.3663548
<ColumnFamily>:<ColumnName>:value
b:cutoutDifference:*binary*
b:cutoutScience:*binary*
b:cutoutTemplate:*binary*
i:aimage: 0.57397074
i:aimagerat: 0.46825972
i:bimage: 0.52000326
i:bimagerat: 0.46825972
i:candid: 1034528221753634751
i:chinr: 0.46825972
i:chipsf: 1.8730389
i:classtar: 0.978
i:clrcoeff: 0.11706493
i:clrcounc: 8.102447E-6
i:clrmed: 0.62865824
i:clrrms: 0.46825972
i:dec: 15.468259749904854
i:decnr: 15.468259749904854
i:diffmaglim: 29.968622
i:distnr: 8.077449
i:distpsnr1: 29.968622
i:distpsnr2: 29.968622
i:distpsnr3: 29.968622
i:drb: 0.999752
i:drbversion: d6_m7
i:dsdiff: -1.18682505E29
i:dsnrms: 9.464858
i:elong: 1.8730389
i:exptime: 29.968622
i:fid: 2
i:field: 568
i:fwhm: 1.8730389
i:isdiffpos: t
i:jd: 2482047.4790935214
i:jdendhist: 2482047.4790935214
i:jdendref: 2482047.478571366
i:jdstarthist: 2482047.4790935214
i:jdstartref: 2482047.4772142665
i:magap: 29.968622
i:magapbig: 29.968622
i:magdiff: -1.1868114E29
i:magfromlim: 0.46825972
i:maggaia: 29.968622
i:maggaiabright: -1.1868139E29
i:magnr: 29.968622
i:magpsf: 29.968622
i:magzpsci: 29.968622
i:magzpscirms: 0.037692007
i:magzpsciunc: 7.1450763E-6
i:mindtoedge: 673.5927
i:nbad: 0
i:ncovhist: 239
i:ndethist: 1
i:neargaia: 29.968622
i:neargaiabright: -1.1868139E29
i:nframesref: 15
i:nid: 1034
i:nmatches: 769
i:nmtchps: 3
i:nneg: 3
i:objectId: ZTF19acmbtud
i:objectidps1: 139540932695408573
i:objectidps2: 139540932846403517
i:objectidps3: 139540932695408573
i:pdiffimfilename: ztf_20191101527951_000568_zr_c04_o_q4_scimrefdiffimg.fits
i:pid: 1029714075307
i:programid: 1
i:programpi: Kulkarni
i:publisher: Fink
i:ra: 128.2831039126012
i:ranr: 128.28545567346546
i:rb: 0.774291
i:rbversion: t17_f5_c3
i:rcid: 15
i:rfid: 569360317
i:schemavsn: 3.3
i:scorr: 11.468259749904854
i:seeratio: 1.8730389
i:sgmag1: -1.1868139E29
i:sgmag2: 29.968622
i:sgmag3: 29.968622
i:sgscore1: 0.0
i:sgscore2: 0.99584574
i:sgscore3: 0.5
i:sharpnr: -1.18682505E29
i:sigmagap: 0.46825972
i:sigmagapbig: 0.46825972
i:sigmagnr: 0.216
i:sigmapsf: 0.15521143
i:simag1: -1.1868139E29
i:simag2: 29.968622
i:simag3: 29.968622
i:sky: 0.5201477
i:srmag1: 29.968622
i:srmag2: 29.968622
i:srmag3: -1.1868139E29
i:ssdistnr: 0.0
i:ssmagnr: 29.968622
i:ssnamenr: 52124
i:ssnrms: 13.6383
i:sumrat: 1.8730389
i:szmag1: -1.1868139E29
i:szmag2: 29.968622
i:szmag3: -1.1868139E29
i:tblid: 22
i:tooflag: 0
i:xpos: 2398.9841
i:ypos: 2400.76
i:zpclrcov: -1.18682505E29
i:zpmed: 29.968622
d:cdsxmatch: Unknown
d:rfscore: 0.0
```

The above row will be translated into graph data model as follows:

### Vertices
- Every alert (single row in hbase) will be modelled as a graph vertex.
- Following alert properties will be modelled as vertex properties hereby referred to as ```link properties```
    - Supernova Random Forest classification ```(d:rfscore)```
    - Supernova Bayesian Neural Network classification ```(d:snn)``` [coming soon]
    - Microlensing Random Forest classification ```(d:mulens_class_i)``` [coming soon]
    - Cross-match label ```(d:cdsxmatch)```
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