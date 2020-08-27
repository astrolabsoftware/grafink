# VertexClassifiers

VertexClassifiers are rules, each of which creates a set of edges in the graph.

Currently supported rules to generate edges are:

| Classifier Name | Description |
|-----------------|-------------|
| SimilarityClassifer | This rule generates an edge between two vertices if the value of their `similarity` expression match |
| SameValueClassifier | This rule generates an edge between two vertices if the value of one of the properties is exactly matching |
| TwoModeClassifier | This rule generates an edge from each vertex to a fixed `similarity` vertex based on a `recipe` attributed to this fixed vertex |

These Classifiers are described in more details in the following sections
 
## SimilarityClassifer

This rule generates an edge between 2 alerts if one or more of the columns match,
as specified by a ```similarity``` expression.
The ```similarity``` expression is described in terms of column names, AND and OR operators.
For eg: given an expression ```(rfscore AND snn_snia_vs_nonia) OR objectId```, the rule will create an edge if
the ```rfscore``` and ```snn_snia_vs_nonia``` match or ```objectId``` match.

The definition for a 'match' varies per column:

| Column Name | Match Condition |
|-------------|-----------------|
|rfscore|<code>rfscore1 > 0.9 && rfscore2 > 0.9</code>|
|snn_snia_vs_nonia|<code>snn_snia_vs_nonia1 > 0.9 && snn_snia_vs_nonia2 > 0.9</code>|
|cdsxmatch|<code>(cdsxmatch1 =!= "Unknown") && (cdsxmatch1 === cdsxmatch2)</code>|
|roid|<code>(roid1, roid2) => (roid1 > 1) && (roid2 > 1)</code>|
|mulens|<code>(mulens1_class_1 === "ML" && mulens1_class_2 === "ML") && (mulens2_class_1 === "ML" && mulens2_class_2 === "ML")</code>|

Here the column name followed by 1 or 2 specifies the column for the first or the second row being matched.

The Edge created by this classifier is labelled ```similarity``` (defined in application.conf) with a property ```value``` which is set to an integer value calculated
by counting the number of matching conditions from the similarity expression amongst the connected vertices.

Since the definition of a match varies per column, this classifier is not generic enough to be used with any dataset, and is specific
to the use case for which grafink was built. But it can be easily extended by defining new match conditions in ```SimilarityExpParser.scala``` file
for the desired columns of your dataset and then changing the ```similarityExp``` config in ```application.conf```

For example here is the resultant edges in a graph with similarity expression as mentioned:

![SimilarityClassifier](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/saucam/grafink/master/docs/classifiers/similarityclassifier.puml)

Here, vertex V1 and V2 are connected by an edge labelled ```similarity``` with ```value``` equal to 3 since they satisfy 3 of the conditions
described by the similarity expression, namely, rfscore condition, snn_snia_vs_nonia condition and objectId condition.
Note that even though rfscore and snn_snia_vs_nonia conditions are ANDed together in the similarity expression, each condition contributes +1 individually to the similarity value.

## TwoModeClassifier

The TwoModeClassifier will add edges between 2 different types of vertices distinguished by their labels:

- similarity vertices: These are defined in a csv file ([```fixedvertices.csv```](../../core/src/main/resources/fixedvertices.csv)) and treated as fixed vertices. This is a very generic file and lets us add any vertex that we want pre-created in the graph before loading data.
Note that these vertices are only loaded if twoModeClassifier is added to ```edgeLoader.rulesToApply``` config in ```application.conf```
Also, if the first vertex defined in ```fixedvertices.csv``` is already created in the graph, the loading of these fixed vertices is skipped.

- alert vertices: These are the actual vertices loaded from the data. It is possible to define multiple vertex labels in the schema, but only one vertex label will be associated with the data loaded vertices.
Here is some sample data from fixedvertices.csv

```csv
1,"similarity","recipe","string","supernova"
2,"similarity","recipe","string","microlensing"
4,"similarity","recipe","string","catalog","equals","string","WD*"
```
Here each row represents a fixed vertex to be loaded. The first column is the vertex id that will be assigned to each vertex.
Note that this id must be unique for each row, and should be less than ```idManager.spark.reservedIdSpace``` config value in ```application.conf```

The second column represents the name of the vertex label, thereafter, each 3 column set describes a vertex property,
where the first such column defines the property name, second column defines the property value type and the third column defines the actual value for the property.
So in the above example,
3rd row will add a fixed vertex with custom id 4, vertex label 'similarity', and 2 vertex properties
recipe - of type string with value "catalog"
and
equals - of type string with value equal to "WD*"

The Edge created by this classifier is labelled ```satr``` (defined in application.conf) with a property weight which is set to 0.0 as it is not being used.
The actual edge creation condition to these fixed vertices is dictated by the value of the ```recipe``` vertex property.
These conditions are again fixed to the ```recipe``` value, and hence this classifier is also specific to the use case of grafink,
but can be easily extended by adding new recipes in ```TwoModeClassifier.scala```

Here are the currently supported recipes and their condition description

| Recipe Name | Condition | Description |
|-------------|-----------|-------------|
| supernova | rfscore > 0.9 && snn_snia_vs_nonia > 0.9 | if both rfscore and snn_snia_vs_nonia are greater than 0.9, edge will be created from that vertex to this fixed vertex |
| microlensing | mulens_class_1 == "ML" && mulens_class_2 == "ML" | If both mulens_class_1 and mulens_class_2 are equal to "ML", edge will be created from that vertex to this fixed vertex |
| asteroids | roid > 1 | If roid value is greater than 1, edge will be created from that vertex to this fixed vertex |
| catalog | cdsxmatch column == value of "equals" property | Any vertex with the cdsxmatch property equal to the value of "equals" property of this fixed vertex will be connected together by an edge. |

For example here is the resultant edges in a graph to which TwoModeClassifier rule has been applied:

![TwoModeClassifier](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/saucam/grafink/master/docs/classifiers/twomodeclassifier.puml)

Here, the 3 fixed vertices are shown in gold along with the normal alert vertices in yellow.

## SameValueClassifier

This classifier will link vertices having exact same vertex property values for a given vertex property for eg: ```objectId```.
This creates edges between the vertices with label ```exactmatch``` (which is defined in ```application.conf```), with a property ```propertname``` which stores the name of the property which is equal in the connected vertices. So if we apply this rule on vertex property ```objectId```, then
each resultant edge will have a property called ```propertyname``` with value set to ```objectId```.
This rule can actually be passed a list of vertex property names, and for each of these properties, a set of edges connecting vertices with same value will be created (with the corresponding ```propertname``` value)

Out of the classifiers described here, this is the most generic, which can be applied over any vertex property of the dataset being loaded, and is not specific to grafink use case.
Note that computing these edges requires an inner join between data to be loaded and existing data in the graph, so need to be careful which property to configure for this classifier.

For example here is the resultant edges in a graph to which SameValueClassfier rule has been applied:

![SameValueClassifier](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/saucam/grafink/master/docs/classifiers/samevalueclassifier.puml)

Observe that each vertex will be connected to all the other vertices having the exact same vertex property value for which this rule is applied.
So in this case V2 is connected to both V3 and V6 and V3 is also connected to both V2 and V6 and so on.
