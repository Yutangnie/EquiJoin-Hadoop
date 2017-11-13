Objective and Assumption: this assignment is to implement an equijoin on hadoop by its built-in map and reduce function.
The input is only one file with both R and S relations in it. The second column of each line in the input is the join column, where the variable type can only be numeric ones.
Implementation
	Driver class: all the configuration about hadoop are set here, including where the input comes from and where the output goes to, what is the input format of mapper and the output format of reducer, as well as the output key and value format for both mapper and reducer. Note here since both the mapper and reducer output the same format of key and value, explicitly indicate the mapper key and value output class is unnecessary. 
	Mapper class: design a new class called JoinMapper which extends Mapper and specify the class type of KeyIn, ValueIn, KeyOut and ValueOut, override the map function. Inside the map function, extract the join key from each input (ValueIn here) and then write out with the key and the whole input value.
	Reducer class: design a new class called JoinReducer which extends Reducer and specify the class type, then override the reduce function. Inside reduce function, first iterate the input value to get all the inputs and stores in a list. Then traverse the list to join each two of them with the same key with condition that they are not coming from the same R or S table.

Useful note: 
how to create jar file?
Use command "jar -cvf equijoin.jar *.class" instead of IDE built-in buttons to generate the jar file, before doing so, need to add the hadoop path to the classpath in ".bashrc" so that can complie the "equijoin.java" with classes from apache hadoop. If use IDE created jar, while submit to hadoop, no need to explicitly indicate the main class name, "equijoin here". But with jar command, the "equijoin" class name is necenessary. The following two lines should be in ".bashrc":
# for hadoop
export HADOOP_HOME=/usr/local/hadoop
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath):$CLASSPATH

Learn about Hadoop:
Each hadoop job must have at least one mapper and one reducer. Of course, it can have many. For limitations of number of mapper and reducer, please refer to hadoop document.
And what a job did is to set the environment of your implementation, namely, the input/output file path, input/output format, as well as input/output key/value format for both mapper and reducer.
use job.setOutputKeyClass() job.setOutputValueClass() to set both reducer and mapper output format.
But if mapper output format is not as same as reducer, need to specifically set by setMapOutputKeyClass() and setMapOutputValueClass().
What mapper does is simply grab the key and value from input splits and write out the expected <key, value> pairs. These pairs will then go to reducer, the shuffle section is to sort the key and rearrange all the values with the same key in an iterator. Then in reducer's reduce function, you can implement your own code to deal with those iterable values.
