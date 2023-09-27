An example/dummy JAR to test custom JARs in Microsoft Fabric - Workspace Management - Data Engineering/Science - Library management : 

![image](images/FabricDEDSLibraryMgmtCustomLibrary.png)



This sample JAR contains a class named **MyLib** which is writing the arguments passed to it into standard output. Compiled JAR (MyLib.jar) is only 787 bytes in size. You can either download and use the compiled JAR or you can use the supplied **MyLib.java** and **META-INF** folder and its content to package into a JAR yourself. 

Reference : [Create a Java JAR part in Use a JAR in an Azure Databricks job article](https://learn.microsoft.com/en-us/azure/databricks/workflows/jobs/how-to/use-jars-in-workflows#create-a-java-jar)
