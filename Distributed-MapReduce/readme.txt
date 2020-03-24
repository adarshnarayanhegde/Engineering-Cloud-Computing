Please follow the following porcedure to run and test MapReduce Applications:

1)For any missing pacages please use pip install...

2) Get the IP address of the machine on which the MapReduce Applications should run and configure it wihtin the ipconfig.json file along with the port number required.

3) After configuring IP and port numbers, execute the shell script "map_reduce.sh"

4) After running map_reduce.sh, run the shell script "spawn_master.sh" as
	i) spawn_master.sh wordConfig.json   -->To run word count application
	ii) spawn_master,sh invertedConfig.json  --> To run inverted index example

5) Once done kill all the processes.
