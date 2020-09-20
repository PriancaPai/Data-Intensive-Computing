============================================================ 
		CSE-587, DIC, LAB - 5   
============================================================

************************Contributors************************

-------------------------------------------------------------
|     Name         | Person No    |   UBIT ID               |
-------------------------------------------------------------
|  Sandeep Shenoy  |	50205705  |   sshenoy@buffalo.edu   |
|  Vipin Kumar	   |	50208397  |   vkumar25@buffalo.edu  |
-------------------------------------------------------------


************************System Setup Start************************

1. Environment : Install the Spark on a VM with RAM of 8 GB

2. Install the Jupyter on the same VM and set the following environment variable
   in your VM's bashrc file.

	# FOR JUPYTER AND ALL 
	export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
	export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$PYTHONPATH
	export PYSPARK_DRIVER_PYTHON=jupyter
	export PYSPARK_DRIVER_PYTHON_OPTS='notebook'

3. Code File Location :
	Folder Location  : "./"
	Titanic vignette : Welcome+to+Spark+with+Python.ipynb
        Task1 Code       : Lab5-Task1.ipynb 

4. Files location : 
   Input file path : "./input/1"
		     "./input/2"
		     a) Here we will create an input folder in the same directory
		        where we have created our ipython notebook.
		     b) Create input folder with numerical sub-folders containing different 
		        number of files for the ease of automation.
		     c) The same file structure is used for 2-Gram and 3 Gram.   
			
   Output file path : 
		
		For 2-Gram
		      "./output2/1"
		      a) As above we will create the output folder structure as 
		         input files structure stated above. 
		      b) The resultant folder will contain a csv file containing the output.


		For 3-Gram
		      "./output3/1"
		      a) As above we will create the output folder structure as 
		         input files structure stated above. 
		      b) The resultant folder will contain a csv file containing the output.

   Input file structure : This structure will be same for both 2-Gram and 3-Gram
		Syntax	: <Location>	Line
   		Example : <prud. epil. 2> pius, fidelis, innocens, pudicus 
			  <prud. epil. 2> --->> This is location 
			  pius, fidelis, innocens, pudicus ---->> this is the input line 

   Output file structure : 
		For 2-Gram       : Stored in a CSV File
			Syntax	 : {word1, word2} Location1 |word1Position,word2Position| , Location2 |word1Position,word2Position|	
			Example  : {a,signum} prud. ham. 2.567 |1.3| , prud. ham. 2.567 |1.5| 	

		For 3-Gram       : Stored in a CSV File
			Syntax	 : {word1, word2, word3} Location1 |word1Position,word2Position,word3Position| , Location2 |word1Position,word2Position,word3Position|	
			Example  : {a,signum,paternis} prud. ham. 2.567 |1.3.6| , prud. ham. 2.567 |1.5.6|	

   Additional file path : 
		Lemmatizer csv : "./new_lemmatizer.csv"
 		Titanic Data   : "./titanic.csv"

************************System Setup End************************

************************Conclusion Start************************

We tested the above code with multiple files and found out that the time duration take for 2-Gram and 3-Gram both increases in a linear fashion for some
time. But after a certain number of files the code just stops which indicates an exponetial rise in the execution time for both the codes.
Hence we can say that there is a linear rise in execution time which booms exponetially later on.

The supporting plot generated above. The graph says the same story as mentioned in J. Lin & C. Dyer. Data intensive text 
processing with MapReduce book.

Plot Location : "./"
Plot Name     : performance.png

************************Conclusion Ends ************************




 
